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

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Utility class for constructing transactionalIds for Kafka transactions. */
@Internal
public class TransactionalIdFactory {
    private static final String TRANSACTIONAL_ID_DELIMITER = "-";
    private static final String ATTEMPT_SCOPE_DELIMITER = "_";
    private static final Pattern TRANSACTIONAL_ID = Pattern.compile("(.*)-\\d+-\\d+");
    private static final Pattern ATTEMPT_SCOPED_PREFIX =
            Pattern.compile("(.*)-(\\d+)_(\\d+)_(\\d+)");

    /**
     * Constructs a transactionalId with the following format {@code
     * transactionalIdPrefix-subtaskId-checkpointOffset}.
     *
     * @param transactionalIdPrefix prefix for the id
     * @param subtaskId describing the subtask which is opening the transaction
     * @param checkpointOffset an always incrementing number usually capturing the number of
     *     checkpoints taken by the subtask
     * @return transactionalId
     */
    public static String buildTransactionalId(
            String transactionalIdPrefix, int subtaskId, long checkpointOffset) {
        return transactionalIdPrefix
                + TRANSACTIONAL_ID_DELIMITER
                + subtaskId
                + TRANSACTIONAL_ID_DELIMITER
                + checkpointOffset;
    }

    /**
     * Constructs the prefix under which one execution attempt of a writer names its transactions:
     * {@code transactionalIdPrefix-restoredCheckpointId_subtaskIndex_attemptNumber}.
     *
     * <p>Kafka lets any producer that is initialized with a transactional id end the transaction
     * that another producer has open under the same id. Two execution attempts of a subtask can be
     * alive at the same time, for example when the JobManager has given up on a TaskManager that
     * still runs. The attempt number tells them apart, so that an attempt can neither reuse nor
     * guess the transactional ids of a later attempt. The subtask index is part of the prefix
     * because attempts are counted per subtask. The checkpoint id keeps the prefix apart from the
     * one of an attempt with the same number that a previous JobManager started from an earlier
     * checkpoint: attempt numbers start again from 0 when a JobManager takes over the job.
     *
     * @param transactionalIdPrefix prefix configured for the sink
     * @param restoredCheckpointId id of the checkpoint the attempt started from
     * @param subtaskIndex index of the subtask that runs the attempt
     * @param attemptNumber attempt number of the subtask
     * @return prefix for the transactional ids of the attempt
     */
    public static String buildAttemptScopedPrefix(
            String transactionalIdPrefix,
            long restoredCheckpointId,
            int subtaskIndex,
            int attemptNumber) {
        return transactionalIdPrefix
                + TRANSACTIONAL_ID_DELIMITER
                + restoredCheckpointId
                + ATTEMPT_SCOPE_DELIMITER
                + subtaskIndex
                + ATTEMPT_SCOPE_DELIMITER
                + attemptNumber;
    }

    /**
     * Returns the attempt of the given prefix if the prefix was built with {@link
     * #buildAttemptScopedPrefix(String, long, int, int)}. Prefixes written by versions that did not
     * scope transactional ids to an attempt have no such suffix.
     */
    public static Optional<AttemptScope> extractAttemptScope(String prefix) {
        final Matcher matcher = ATTEMPT_SCOPED_PREFIX.matcher(prefix);
        if (!matcher.matches()) {
            return Optional.empty();
        }
        try {
            return Optional.of(
                    new AttemptScope(
                            matcher.group(1),
                            Long.parseLong(matcher.group(2)),
                            Integer.parseInt(matcher.group(3)),
                            Integer.parseInt(matcher.group(4))));
        } catch (NumberFormatException e) {
            // digits that do not fit the type were not written by this class
            return Optional.empty();
        }
    }

    public static int extractSubtaskId(String name) {
        int lastSep = name.lastIndexOf("-");
        int secondLastSep = name.lastIndexOf("-", lastSep - 1);
        String subtaskString = name.substring(secondLastSep + 1, lastSep);
        return Integer.parseInt(subtaskString);
    }

    public static String extractPrefix(String name) {
        int lastSep = name.lastIndexOf("-");
        int secondLastSep = name.lastIndexOf("-", lastSep - 1);
        return name.substring(0, secondLastSep);
    }

    /**
     * Returns the prefix of the given transactional id if the id has the format of {@link
     * #buildTransactionalId(String, int, long)}. Unlike {@link #extractPrefix(String)}, this method
     * accepts the id of any application.
     */
    public static Optional<String> findPrefix(String name) {
        final Matcher matcher = TRANSACTIONAL_ID.matcher(name);
        return matcher.matches() ? Optional.of(matcher.group(1)) : Optional.empty();
    }

    /** The parts of a prefix that is scoped to one execution attempt of a writer. */
    public static final class AttemptScope {
        private final String transactionalIdPrefix;
        private final long restoredCheckpointId;
        private final int subtaskIndex;
        private final int attemptNumber;

        private AttemptScope(
                String transactionalIdPrefix,
                long restoredCheckpointId,
                int subtaskIndex,
                int attemptNumber) {
            this.transactionalIdPrefix = transactionalIdPrefix;
            this.restoredCheckpointId = restoredCheckpointId;
            this.subtaskIndex = subtaskIndex;
            this.attemptNumber = attemptNumber;
        }

        /** The prefix configured for the sink. */
        public String getTransactionalIdPrefix() {
            return transactionalIdPrefix;
        }

        public long getRestoredCheckpointId() {
            return restoredCheckpointId;
        }

        public int getSubtaskIndex() {
            return subtaskIndex;
        }

        public int getAttemptNumber() {
            return attemptNumber;
        }
    }
}
