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

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.kafka.sink.internal.TransactionAbortStrategyImpl;
import org.apache.flink.connector.kafka.sink.internal.TransactionNamingStrategyImpl;

/**
 * The strategy to name transactions. Naming strategy has implications on the resource consumption
 * on the broker because each unique transaction name requires the broker to keep some metadata in
 * memory for 7 days.
 *
 * <p>All naming strategies use the format {@code transactionalIdPrefix-subtask-offset} where offset
 * is calculated differently.
 */
@PublicEvolving
public enum TransactionNamingStrategy {
    /**
     * The offset of the transaction name is a monotonically increasing number that mostly
     * corresponds to the checkpoint id. This strategy is wasteful in terms of resource consumption
     * on the broker.
     *
     * <p>This is exactly the same behavior as in flink-connector-kafka 3.X.
     *
     * <p>Switching to this strategy from {@link #POOLING} is not supported.
     *
     * <p>When a writer starts, it aborts the transactions that earlier attempts did not finish.
     * With this strategy it finds them by re-initializing the transactional ids that earlier
     * attempts may have used since the restored checkpoint. Each subtask goes through the subtask
     * numbers it is responsible for and ends at the first number for which it finds no used
     * transactional id. It therefore misses the transactions of higher subtask numbers if a number
     * on its way was never used since the restored checkpoint. An example: an attempt ends before
     * its first checkpoint while some of its subtasks have not created their writer yet, a writer
     * with a higher subtask number is lost without being closed, and the next attempt runs with
     * fewer subtasks.
     *
     * <p>A missed transaction remains unfinished until a later attempt comes across it or, at the
     * latest, until the broker aborts it once its transaction timeout has elapsed. During that time
     * the last stable offset of the partitions it wrote to does not advance, so {@code
     * read_committed} consumers receive nothing from behind it. The output of the job stays
     * complete, because the restarted job writes the records of such a transaction again. What
     * these consumers get to read is not protected in the same way: Kafka applies the retention of
     * a topic to all log segments below the high watermark, and an unfinished transaction does not
     * keep the segments that follow it. If the time or size limit of the topic's retention ({@code
     * retention.ms}, {@code retention.bytes}) is reached while the transaction is unfinished,
     * records that were committed after its first record can be deleted before these consumers were
     * allowed to read them. {@link #POOLING} finds such a transaction because it asks the brokers
     * for the unfinished transactions.
     */
    INCREMENTING(TransactionNamingStrategyImpl.INCREMENTING, TransactionAbortStrategyImpl.PROBING),

    /**
     * This strategy reuses transaction names. It is more resource-friendly than {@link
     * #INCREMENTING} on the Kafka broker.
     *
     * <p>It's a new strategy introduced in flink-connector-kafka 4.X. It requires Kafka 3.0+ and
     * additional read permissions on the target topics. The target topics must also be known when a
     * writer starts: {@code setTopic(String)} of the {@link KafkaRecordSerializationSchemaBuilder}
     * provides them, a {@link TopicSelector} only if it also implements {@code
     * KafkaDatasetIdentifierProvider}, and a serializer of your own only if it implements {@code
     * KafkaDatasetFacetProvider} and returns its topics or a topic pattern.
     *
     * <p>The recommended way to switch to this strategy is to first take a checkpoint with
     * flink-connector-kafka 4.X and then switch to this strategy. This will ensure that no
     * transactions are left open from the previous run. Alternatively, you can use a savepoint from
     * any version.
     */
    POOLING(TransactionNamingStrategyImpl.POOLING, TransactionAbortStrategyImpl.LISTING);

    /**
     * The default transaction naming strategy. Currently set to {@link #INCREMENTING}, which is the
     * same behavior of flink-connector-kafka 3.X.
     */
    public static final TransactionNamingStrategy DEFAULT = INCREMENTING;

    /**
     * The backing implementation of the transaction naming strategy. Separation allows to avoid
     * leaks of internal classes in signatures.
     */
    private final TransactionNamingStrategyImpl impl;

    /**
     * The set of supported abort strategies for this naming strategy. Some naming strategies may
     * not support all abort strategies.
     */
    private final TransactionAbortStrategyImpl abortImpl;

    TransactionNamingStrategy(
            TransactionNamingStrategyImpl impl, TransactionAbortStrategyImpl abortImpl) {
        this.impl = impl;
        this.abortImpl = abortImpl;
    }

    @Internal
    TransactionAbortStrategyImpl getAbortImpl() {
        return abortImpl;
    }

    @Internal
    TransactionNamingStrategyImpl getImpl() {
        return impl;
    }
}
