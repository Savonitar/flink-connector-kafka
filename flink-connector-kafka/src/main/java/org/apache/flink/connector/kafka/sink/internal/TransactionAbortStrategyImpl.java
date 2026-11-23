/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kafka.sink.internal;

import org.apache.flink.annotation.Internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Implementations of an abort strategy for transactions left over from previous runs.
 *
 * <p>A writer names its transactions under a prefix that is scoped to its execution attempt, see
 * {@link TransactionalIdFactory#buildAttemptScopedPrefix(String, long, int, int)}. Both strategies
 * abort transactions under the prefixes that the writer knows to belong to an execution that came
 * before it: the prefixes in its recovered state and the prefixes of earlier attempts of its
 * subtask that started from the same checkpoint. They also abort under the prefix of the writer
 * itself, which only an attempt of an earlier run of the job or of a previous JobManager can have
 * used. They never construct or match the prefix of a later attempt, so a writer that is created
 * late cannot end a transaction that a later attempt has open. Transactions of other executions are
 * left to the transaction timeout.
 */
@Internal
public enum TransactionAbortStrategyImpl {
    /**
     * The probing strategy starts with aborting a set of known transactional ids from the recovered
     * state and then continues guessing if more transactions may have been opened between this run
     * and the last successful checkpoint. This also accounts for rescaling in between the attempts.
     *
     * <p>However, the probing is not side-effect free, which leads to an ever-increasing search
     * space for the next probing attempt in case of a restart loop. It will fix eventually on the
     * next successful checkpoints. It's recommended to use this strategy only with a strict restart
     * policy that prevents tight restart loops (e.g. incremental backoff or hard failure after X
     * attempts).
     *
     * <p>Under a prefix without an attempt scope, this is exactly the same behavior as in
     * flink-connector-kafka 3.X.
     */
    PROBING {
        @Override
        public void abortTransactions(Context context) {
            for (String prefix : context.getPrefixesToAbort()) {
                abortTransactionsWithPrefix(prefix, context);
            }

            // Under an attempt-scoped prefix only the writer that the prefix belongs to has named
            // transactions. The subtask ids are known, so there is nothing to extrapolate.
            abortTransactionOfSubtask(
                    context.getAttemptScopedPrefix(), context.getCurrentSubtaskId(), context);
            for (Map.Entry<String, Set<Integer>> restored :
                    context.getRestoredAttemptScopedPrefixes().entrySet()) {
                for (int subtaskId : restored.getValue()) {
                    abortTransactionOfSubtask(restored.getKey(), subtaskId, context);
                }
            }
            for (String prefix : context.getPrefixesOfEarlierAttempts()) {
                if (abortTransactionOfSubtask(prefix, context.getCurrentSubtaskId(), context) > 0) {
                    // That attempt has opened its first transaction, which a writer does after it
                    // has aborted the transactions of the attempts before it.
                    break;
                }
            }
        }

        /**
         * Aborts all transactions that have been created by this subtask in a previous run.
         *
         * <p>It also aborts transactions from subtasks that may have been removed because of
         * downscaling.
         *
         * <p>When Flink downscales X subtasks to Y subtasks, then subtask i is responsible for
         * cleaning all subtasks j in [0; X), where j % Y = i. For example, if we downscale to 2,
         * then subtask 0 is responsible for all even and subtask 1 for all odd subtasks.
         */
        private void abortTransactionsWithPrefix(String prefix, Context context) {
            for (int subtaskId = context.getCurrentSubtaskId();
                    ;
                    subtaskId += context.getCurrentParallelism()) {
                if (abortTransactionOfSubtask(prefix, subtaskId, context) == 0) {
                    // If Flink didn't abort any transaction for current subtask, then we assume
                    // that no
                    // such subtask existed and no subtask with a higher number as well.
                    break;
                }
            }
        }

        /**
         * Aborts all transactions that have been created by a subtask in a previous run after the
         * given checkpoint id.
         *
         * <p>We assume that transaction ids are consecutively used and thus Flink can stop aborting
         * as soon as Flink notices that a particular transaction id was unused.
         */
        private int abortTransactionOfSubtask(String prefix, int subtaskId, Context context) {
            int numTransactionAborted = 0;
            for (long checkpointId = context.getStartCheckpointId();
                    ;
                    checkpointId++, numTransactionAborted++) {
                // initTransactions fences all old transactions with the same id by bumping the
                // epoch
                String transactionalId =
                        TransactionalIdFactory.buildTransactionalId(
                                prefix, subtaskId, checkpointId);
                int epoch = context.getTransactionAborter().abortTransaction(transactionalId);
                // An epoch of 0 indicates that the id was unused before
                if (epoch == 0) {
                    // Note that the check works beyond transaction log timeouts and just depends on
                    // the
                    // retention of the transaction topic (typically 7d). Any transaction that is
                    // not in
                    // the that topic anymore is also not lingering (i.e., it will not block
                    // downstream
                    // from reading)
                    // This method will only cease to work if transaction log timeout = topic
                    // retention
                    // and a user didn't restart the application for that period of time. Then the
                    // first
                    // transactions would vanish from the topic while later transactions are still
                    // lingering until they are cleaned up by Kafka. Then the user has to wait until
                    // the
                    // other transactions are timed out (which shouldn't take too long).
                    break;
                }
            }
            return numTransactionAborted;
        }
    },
    LISTING {
        @Override
        public void abortTransactions(Context context) {
            Collection<String> openTransactionsForTopics = context.getOpenTransactionsForTopics();

            if (openTransactionsForTopics.isEmpty()) {
                return;
            }

            Set<String> prefixesOfThisSubtask =
                    new HashSet<>(context.getPrefixesOfEarlierAttempts());
            prefixesOfThisSubtask.add(context.getAttemptScopedPrefix());

            List<String> openTransactionsForSubtask =
                    openTransactionsForTopics.stream()
                            .filter(name -> isAbortable(name, prefixesOfThisSubtask, context))
                            .collect(Collectors.toList());

            LOG.warn(
                    "Found {} open transactions for subtask {}: {}",
                    openTransactionsForSubtask.size(),
                    context.getCurrentSubtaskId(),
                    openTransactionsForSubtask);
            // look only at transactions coming from this application
            // remove transactions that are owned by the committer
            TransactionAborter transactionAborter = context.getTransactionAborter();
            for (String name : openTransactionsForSubtask) {
                if (context.getPrecommittedTransactionalIds().contains(name)) {
                    LOG.debug(
                            "Skipping transaction {} because it's in the list of transactions to be committed",
                            name);
                    continue;
                }
                transactionAborter.abortTransaction(name);
            }
        }

        /**
         * A transaction can be aborted if its prefix is known to belong to an execution before this
         * writer. The prefix has to match exactly: every attempt of every subtask has a prefix of
         * its own, and they all start with the configured prefix.
         */
        private boolean isAbortable(
                String name, Set<String> prefixesOfThisSubtask, Context context) {
            // ignore transactions from other applications
            Optional<String> prefix = TransactionalIdFactory.findPrefix(name);
            if (prefix.isEmpty()) {
                return false;
            }
            // transactions of this attempt and of earlier attempts of this subtask
            if (prefixesOfThisSubtask.contains(prefix.get())) {
                return true;
            }
            // The writers of the recovered state may have been redistributed over the subtasks,
            // so look only at transactions owned by this subtask
            return (context.getPrefixesToAbort().contains(prefix.get())
                            || context.getRestoredAttemptScopedPrefixes().containsKey(prefix.get()))
                    && context.ownsTransactionalId(name);
        }
    };

    private static final Logger LOG = LoggerFactory.getLogger(TransactionAbortStrategyImpl.class);

    /** Aborts all transactions that have been created by this subtask in a previous run. */
    public abstract void abortTransactions(Context context);

    /** Injects the actual abortion of the transactional id generated by one of the strategies. */
    public interface TransactionAborter {
        int abortTransaction(String transactionalId);
    }

    /** Context for the {@link TransactionAbortStrategyImpl}. */
    public interface Context {
        /**
         * Returns the list of all open transactions for the topics retrieved through introspection.
         */
        Collection<String> getOpenTransactionsForTopics();

        int getCurrentSubtaskId();

        int getCurrentParallelism();

        /**
         * Subtask must abort transactions that they own and must not abort any transaction that
         * they don't own. Ownership is defined as follows:
         *
         * <ul>
         *   <li>Writer states contains the old subtask id and the max parallelism when it was
         *       snapshotted.
         *   <li>On recovery, the state is reassigned somewhere. The new assignee takes the
         *       ownership.
         *   <li>If there was a downscale, one subtask may own several old subtask ids.
         *   <li>If there was an upscale, the ids usually remain stable but that isn't necessary for
         *       the abort to work.
         *   <li>Any number of intermediate attempts between the recovered checkpoint and the
         *       current attempt may produce transactions > then the current parallelism. In that
         *       case, subtask ids are distributed in a round robin fashion using modulo.
         * </ul>
         */
        boolean ownsTransactionalId(String transactionalId);

        /**
         * Returns the prefixes without an attempt scope: prefixes that the recovered state got from
         * a version that did not scope transactional ids to an attempt. A subtask finds the
         * transactions of such a prefix by its subtask id.
         */
        Set<String> getPrefixesToAbort();

        /** Returns the prefix under which this attempt of the writer names its transactions. */
        String getAttemptScopedPrefix();

        /**
         * Returns the prefixes of the earlier attempts of this subtask that started from the same
         * checkpoint as this attempt, the latest attempt first.
         */
        List<String> getPrefixesOfEarlierAttempts();

        /**
         * Returns the attempt-scoped prefixes of the recovered state, each with the ids of the
         * subtasks that the state names for it.
         */
        Map<String, Set<Integer>> getRestoredAttemptScopedPrefixes();

        /**
         * Returns a list of transactional ids that shouldn't be aborted because they are part of
         * the committer state.
         */
        Set<String> getPrecommittedTransactionalIds();

        long getStartCheckpointId();

        TransactionAborter getTransactionAborter();
    }
}
