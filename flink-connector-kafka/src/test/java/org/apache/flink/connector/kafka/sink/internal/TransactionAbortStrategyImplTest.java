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

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.api.connector.sink2.InitContext.INITIAL_CHECKPOINT_ID;
import static org.apache.flink.connector.kafka.sink.internal.TransactionAbortStrategyImpl.LISTING;
import static org.apache.flink.connector.kafka.sink.internal.TransactionAbortStrategyImpl.PROBING;
import static org.apache.flink.connector.kafka.sink.internal.TransactionalIdFactory.buildAttemptScopedPrefix;
import static org.apache.flink.connector.kafka.sink.internal.TransactionalIdFactory.buildTransactionalId;
import static org.apache.flink.connector.kafka.sink.internal.TransactionalIdFactory.extractSubtaskId;
import static org.assertj.core.api.Assertions.assertThat;

class TransactionAbortStrategyImplTest {

    private static final String PREFIX = "prefix";

    @Nested
    class Listing {
        @Test
        void testDownscale() {
            TestContext testContext = new TestContext();
            testContext.setSubtaskIdAndParallelism(0, 2);
            testContext.setOwnedSubtaskIdsAndMaxParallelism(0, 1, 4);

            testContext.addPrecommittedTransactionalIds(0, 1L);
            testContext.addPrecommittedTransactionalIds(1, 1L);
            testContext.addOpenTransaction(2, 1L); // not owned
            String t02 = testContext.addOpenTransaction(0, 2L);
            String t12 = testContext.addOpenTransaction(1, 2L);
            testContext.addOpenTransaction(2, 2L); // not owned

            LISTING.abortTransactions(testContext);
            assertThat(testContext.getAbortedTransactions()).containsExactlyInAnyOrder(t02, t12);
        }

        @Test
        void testDownscaleWithUnsafeTransactionalIds() {
            TestContext testContext = new TestContext();
            testContext.setSubtaskIdAndParallelism(1, 2);
            testContext.setOwnedSubtaskIdsAndMaxParallelism(2, 3, 4);

            String t11 = testContext.addOpenTransaction(1, 1L); // not owned
            testContext.addPrecommittedTransactionalIds(2, 1L);
            testContext.addPrecommittedTransactionalIds(3, 1L);
            String t12 = testContext.addOpenTransaction(1, 2L); // not owned
            String t22 = testContext.addOpenTransaction(2, 2L);
            String t32 = testContext.addOpenTransaction(3, 2L);

            LISTING.abortTransactions(testContext);
            assertThat(testContext.getAbortedTransactions()).containsExactlyInAnyOrder(t22, t32);
        }

        @Test
        void testDownscaleWithIntermediateUpscale() {
            TestContext testContext = new TestContext();
            testContext.setSubtaskIdAndParallelism(0, 2);
            testContext.setOwnedSubtaskIdsAndMaxParallelism(0, 1, 4);

            testContext.addPrecommittedTransactionalIds(0, 1L);
            testContext.addPrecommittedTransactionalIds(1, 1L);
            String t02 = testContext.addOpenTransaction(0, 2L);
            String t12 = testContext.addOpenTransaction(1, 2L);
            String t52 = testContext.addOpenTransaction(5, 2L);
            testContext.addOpenTransaction(6, 2L); // not owned

            LISTING.abortTransactions(testContext);
            assertThat(testContext.getAbortedTransactions())
                    .containsExactlyInAnyOrder(t02, t12, t52);
        }

        @Test
        void testUpscale() {
            TestContext testContext = new TestContext();
            testContext.setSubtaskIdAndParallelism(0, 4);
            testContext.setOwnedSubtaskIdsAndMaxParallelism(0, 2);

            testContext.addPrecommittedTransactionalIds(0, 1L);
            testContext.addOpenTransaction(1, 1L); // not owned
            String t02 = testContext.addOpenTransaction(0, 2L);
            testContext.addOpenTransaction(1, 2L); // not owned

            LISTING.abortTransactions(testContext);
            assertThat(testContext.getAbortedTransactions()).containsExactlyInAnyOrder(t02);
        }

        @Test
        void testUpscaleWithIntermediateUpscale() {
            TestContext testContext = new TestContext();
            testContext.setSubtaskIdAndParallelism(0, 4);
            testContext.setOwnedSubtaskIdsAndMaxParallelism(0, 2);

            testContext.addPrecommittedTransactionalIds(0, 1L);
            String t02 = testContext.addOpenTransaction(0, 2L);
            String t42 = testContext.addOpenTransaction(4, 2L);
            testContext.addOpenTransaction(5, 2L); // not owned

            LISTING.abortTransactions(testContext);
            assertThat(testContext.getAbortedTransactions()).containsExactlyInAnyOrder(t02, t42);
        }

        @Test
        void testUpscaleWithNoState() {
            TestContext testContext = new TestContext();
            testContext.setSubtaskIdAndParallelism(3, 4);
            testContext.setOwnedSubtaskIdsAndMaxParallelism(3, 4);

            String t31 = testContext.addOpenTransaction(3, 1L);
            String t32 = testContext.addOpenTransaction(3, 2L);

            LISTING.abortTransactions(testContext);
            assertThat(testContext.getAbortedTransactions()).containsExactlyInAnyOrder(t31, t32);
        }

        @Test
        void testAbortsEarlierAttemptsOfSameCheckpoint() {
            TestContext testContext = new TestContext();
            testContext.setSubtaskIdAndParallelism(0, 2);
            testContext.setOwnedSubtaskIdsAndMaxParallelism(0, 2);
            testContext.setAttempt(5, 0, 2);

            // an attempt of a previous JobManager that had the scope of this attempt
            String own = testContext.addOpenTransaction(testContext.getAttemptScopedPrefix(), 0);
            String earlier1 = testContext.addOpenTransaction(scope(5, 0, 1), 0);
            // an earlier attempt of this subtask owns every transaction under its prefix
            String earlier0 = testContext.addOpenTransaction(scope(5, 0, 0), 1);

            LISTING.abortTransactions(testContext);
            assertThat(testContext.getAbortedTransactions())
                    .containsExactlyInAnyOrder(own, earlier1, earlier0);
        }

        @Test
        void testSparesLaterAttemptsAndOtherExecutions() {
            TestContext testContext = new TestContext();
            testContext.clearPrefixesToAbort();
            testContext.setSubtaskIdAndParallelism(0, 2);
            testContext.setOwnedSubtaskIdsAndMaxParallelism(0, 2);
            testContext.setAttempt(5, 0, 2);

            testContext.addOpenTransaction(scope(5, 0, 3), 0); // later attempt
            testContext.addOpenTransaction(scope(5, 0, 10), 0); // starts with an earlier prefix
            testContext.addOpenTransaction(scope(6, 0, 0), 0); // started from a later checkpoint
            testContext.addOpenTransaction(scope(4, 0, 1), 0); // not in the recovered state
            testContext.addOpenTransaction(scope(5, 1, 0), 0); // attempt of another subtask
            testContext.addOpenTransaction(PREFIX, 0); // no scope, not in the recovered state

            LISTING.abortTransactions(testContext);
            assertThat(testContext.getAbortedTransactions()).isEmpty();
        }

        @Test
        void testAbortsOwnedTransactionsOfRecoveredAttempt() {
            TestContext testContext = new TestContext();
            testContext.setSubtaskIdAndParallelism(0, 2);
            testContext.setOwnedSubtaskIdsAndMaxParallelism(0, 2);
            testContext.setAttempt(5, 0, 2);
            String recovered = scope(3, 0, 1);
            testContext.addRestoredAttemptScopedPrefix(recovered, 0);

            testContext.addPrecommittedTransactionalId(recovered, 0, 0);
            String open = testContext.addOpenTransaction(recovered, 0, 1);
            // the state of the writer was split: another subtask owns this transaction
            testContext.addOpenTransaction(recovered, 1, 0);

            LISTING.abortTransactions(testContext);
            assertThat(testContext.getAbortedTransactions()).containsExactly(open);
        }

        @Test
        void testIgnoresTransactionsOfOtherApplications() {
            TestContext testContext = new TestContext();
            testContext.setSubtaskIdAndParallelism(0, 1);
            testContext.setOwnedSubtaskIdsAndMaxParallelism(0, 1);

            testContext.addOpenTransactionalId("other-application");
            testContext.addOpenTransactionalId(PREFIX);
            testContext.addOpenTransactionalId(PREFIX + "-a-b");

            LISTING.abortTransactions(testContext);
            assertThat(testContext.getAbortedTransactions()).isEmpty();
        }
    }

    @Nested
    class Probing {
        @Test
        void testProbesPrefixWithoutScopeBySubtaskId() {
            TestContext testContext = new TestContext();
            testContext.setSubtaskIdAndParallelism(1, 2);
            testContext.setStartCheckpointId(6);

            // subtask 3 was removed by downscaling
            testContext.addUsedTransactionalIds(PREFIX, 1, 6, 7);
            testContext.addUsedTransactionalIds(PREFIX, 3, 6);

            PROBING.abortTransactions(testContext);
            assertThat(testContext.getAbortedTransactions())
                    .contains(
                            buildTransactionalId(PREFIX, 1, 6),
                            buildTransactionalId(PREFIX, 1, 7),
                            buildTransactionalId(PREFIX, 3, 6));
        }

        @Test
        void testProbesRecoveredAttemptWithItsSubtaskId() {
            TestContext testContext = new TestContext();
            testContext.clearPrefixesToAbort();
            testContext.setSubtaskIdAndParallelism(0, 1);
            testContext.setAttempt(5, 0, 2);
            testContext.setStartCheckpointId(6);
            // the writer of subtask 1 wrote the state that subtask 0 recovered after downscaling
            String recovered = scope(3, 1, 0);
            testContext.addRestoredAttemptScopedPrefix(recovered, 1);
            testContext.addUsedTransactionalIds(recovered, 1, 6, 7);

            PROBING.abortTransactions(testContext);
            assertThat(testContext.getAbortedTransactions())
                    .containsExactlyInAnyOrder(
                            // the first unused id ends the probing of a prefix
                            buildTransactionalId(testContext.getAttemptScopedPrefix(), 0, 6),
                            buildTransactionalId(recovered, 1, 6),
                            buildTransactionalId(recovered, 1, 7),
                            buildTransactionalId(recovered, 1, 8),
                            // the earlier attempts of subtask 0 have not opened a transaction
                            buildTransactionalId(scope(5, 0, 1), 0, 6),
                            buildTransactionalId(scope(5, 0, 0), 0, 6));
        }

        @Test
        void testStopsAtEarlierAttemptThatStartedItsWriter() {
            TestContext testContext = new TestContext();
            testContext.clearPrefixesToAbort();
            testContext.setSubtaskIdAndParallelism(0, 1);
            testContext.setAttempt(5, 0, 3);
            testContext.setStartCheckpointId(6);
            // attempt 2 failed before it created its writer, attempt 1 has aborted for attempt 0
            testContext.addUsedTransactionalIds(scope(5, 0, 1), 0, 6);
            testContext.addUsedTransactionalIds(scope(5, 0, 0), 0, 6);

            PROBING.abortTransactions(testContext);
            assertThat(testContext.getAbortedTransactions())
                    .containsExactlyInAnyOrder(
                            buildTransactionalId(testContext.getAttemptScopedPrefix(), 0, 6),
                            buildTransactionalId(scope(5, 0, 2), 0, 6),
                            buildTransactionalId(scope(5, 0, 1), 0, 6),
                            buildTransactionalId(scope(5, 0, 1), 0, 7));
        }

        @Test
        void testNeverProbesLaterAttempt() {
            TestContext testContext = new TestContext();
            testContext.clearPrefixesToAbort();
            testContext.setSubtaskIdAndParallelism(0, 1);
            testContext.setAttempt(5, 0, 1);
            testContext.setStartCheckpointId(6);
            // a later attempt runs and has opened its transactions
            testContext.addUsedTransactionalIds(scope(5, 0, 2), 0, 6, 7);
            testContext.addUsedTransactionalIds(scope(5, 0, 0), 0, 6);

            PROBING.abortTransactions(testContext);
            assertThat(testContext.getAbortedTransactions())
                    .isNotEmpty()
                    .noneMatch(id -> id.startsWith(scope(5, 0, 2)));
        }
    }

    private static String scope(long restoredCheckpointId, int subtaskIndex, int attemptNumber) {
        return buildAttemptScopedPrefix(PREFIX, restoredCheckpointId, subtaskIndex, attemptNumber);
    }

    static final class TestContext implements TransactionAbortStrategyImpl.Context {
        private final Set<Integer> ownedSubtaskIds = new HashSet<>();
        private final Set<String> precommittedTransactionalIds = new HashSet<>();
        private final Collection<String> abortedTransactions = new ArrayList<>();
        private final Collection<String> openTransactionalIds = new ArrayList<>();
        private int currentSubtaskId;
        private int currentParallelism;
        private int maxParallelism;
        private String prefix = PREFIX;
        private final Set<String> prefixesToAbort = new HashSet<>(Set.of(prefix));
        private String attemptScopedPrefix = scope(0, 0, 0);
        private final Set<String> usedTransactionalIds = new HashSet<>();
        private final List<String> prefixesOfEarlierAttempts = new ArrayList<>();
        private final Map<String, Set<Integer>> restoredAttemptScopedPrefixes = new HashMap<>();
        private long startCheckpointId = INITIAL_CHECKPOINT_ID;

        public void setPrefix(String prefix) {
            this.prefix = prefix;
            this.prefixesToAbort.add(prefix);
        }

        public Collection<String> getAbortedTransactions() {
            return abortedTransactions;
        }

        /** Sets the scope of the attempt and its earlier attempts on the same checkpoint. */
        public void setAttempt(long restoredCheckpointId, int subtaskIndex, int attemptNumber) {
            this.attemptScopedPrefix = scope(restoredCheckpointId, subtaskIndex, attemptNumber);
            this.prefixesOfEarlierAttempts.clear();
            for (int attempt = attemptNumber - 1; attempt >= 0; attempt--) {
                this.prefixesOfEarlierAttempts.add(
                        scope(restoredCheckpointId, subtaskIndex, attempt));
            }
        }

        public void addRestoredAttemptScopedPrefix(String prefix, int subtaskId) {
            restoredAttemptScopedPrefixes
                    .computeIfAbsent(prefix, p -> new HashSet<>())
                    .add(subtaskId);
        }

        public void clearPrefixesToAbort() {
            prefixesToAbort.clear();
        }

        /** Ids that a producer has been initialized with; probing gets a non-zero epoch. */
        public void addUsedTransactionalIds(String prefix, int subtaskId, long... offsets) {
            for (long offset : offsets) {
                usedTransactionalIds.add(buildTransactionalId(prefix, subtaskId, offset));
            }
        }

        public String addOpenTransaction(String prefix, int subtaskId) {
            return addOpenTransaction(prefix, subtaskId, 0);
        }

        public String addOpenTransaction(String prefix, int subtaskId, long offset) {
            return addOpenTransactionalId(buildTransactionalId(prefix, subtaskId, offset));
        }

        public String addOpenTransactionalId(String transactionalId) {
            openTransactionalIds.add(transactionalId);
            return transactionalId;
        }

        public void addPrecommittedTransactionalId(String prefix, int subtaskId, long offset) {
            precommittedTransactionalIds.add(addOpenTransaction(prefix, subtaskId, offset));
        }

        public String addPrecommittedTransactionalIds(int oldSubtaskId, long checkpointId) {
            String transactionalId = addOpenTransaction(oldSubtaskId, checkpointId);
            precommittedTransactionalIds.add(transactionalId);
            return transactionalId;
        }

        public String addOpenTransaction(int oldSubtaskId, long checkpointId) {
            String transactionalId = getTransactionalId(oldSubtaskId, checkpointId);
            openTransactionalIds.add(transactionalId);
            return transactionalId;
        }

        @Override
        public int getCurrentSubtaskId() {
            return this.currentSubtaskId;
        }

        @Override
        public int getCurrentParallelism() {
            return this.currentParallelism;
        }

        @Override
        public boolean ownsTransactionalId(String transactionalId) {
            return ownedSubtaskIds.contains(extractSubtaskId(transactionalId) % maxParallelism);
        }

        @Override
        public Set<String> getPrefixesToAbort() {
            return this.prefixesToAbort;
        }

        @Override
        public String getAttemptScopedPrefix() {
            return this.attemptScopedPrefix;
        }

        @Override
        public List<String> getPrefixesOfEarlierAttempts() {
            return this.prefixesOfEarlierAttempts;
        }

        @Override
        public Map<String, Set<Integer>> getRestoredAttemptScopedPrefixes() {
            return this.restoredAttemptScopedPrefixes;
        }

        @Override
        public Set<String> getPrecommittedTransactionalIds() {
            return this.precommittedTransactionalIds;
        }

        @Override
        public long getStartCheckpointId() {
            return this.startCheckpointId;
        }

        public void setStartCheckpointId(long startCheckpointId) {
            this.startCheckpointId = startCheckpointId;
        }

        @Override
        public TransactionAbortStrategyImpl.TransactionAborter getTransactionAborter() {
            return transactionalId -> {
                abortedTransactions.add(transactionalId);
                return usedTransactionalIds.contains(transactionalId) ? 1 : 0;
            };
        }

        @Override
        public Collection<String> getOpenTransactionsForTopics() {
            return this.openTransactionalIds;
        }

        public void setSubtaskIdAndParallelism(int newSubtaskId, int newParallelism) {
            this.currentSubtaskId = newSubtaskId;
            this.currentParallelism = newParallelism;
        }

        public void setOwnedSubtaskIdsAndMaxParallelism(int... subtaskIdsOrParallelism) {
            for (int index = 0; index < subtaskIdsOrParallelism.length; index++) {
                if (index < subtaskIdsOrParallelism.length - 1) {
                    this.ownedSubtaskIds.add(subtaskIdsOrParallelism[index]);
                } else {
                    this.maxParallelism = subtaskIdsOrParallelism[index];
                }
            }
        }

        private String getTransactionalId(int oldSubtaskId, long checkpointId) {
            return TransactionalIdFactory.buildTransactionalId(prefix, oldSubtaskId, checkpointId);
        }
    }
}
