/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kafka.sink.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaCommittable;
import org.apache.flink.connector.kafka.sink.KafkaSink;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.function.BiFunction;

/**
 * The committer of a {@link KafkaSink} that does not write in transactions.
 *
 * <p>The writer of such a sink creates no committables. The only committables that reach this
 * committer come from restored state: a snapshot that an execution with {@link
 * DeliveryGuarantee#EXACTLY_ONCE} took holds the transactions with the records of the last
 * checkpoint interval before the snapshot. They can still be open, for example after a savepoint
 * that did not stop the job. The snapshot covers their records, so they are committed here;
 * otherwise the broker aborts them once they time out.
 *
 * <p>The transactions are committed as {@link KafkaCommitter} commits them, with one difference: a
 * commit that fails for an unknown reason is logged and reported as failed, and does not fail the
 * job. This is a trade-off. Some of these failures cannot be repaired, for example when the broker
 * no longer knows the transactional id; failing the job for them would keep a sink without
 * transactions from ever getting past the restore. Others can be repaired by an operator, for
 * example a missing permission for the transactional id; for those, going on loses the records of
 * the transaction, where a failed job could have committed them after the repair.
 */
@Internal
public class RestoredTransactionCommitter implements Committer<KafkaCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(RestoredTransactionCommitter.class);

    /**
     * Settings for sending records that are valid for a sink without transactions and that a
     * transactional producer rejects. The committing producer sends no records, so it runs with the
     * defaults instead.
     */
    private static final List<String> SETTINGS_REJECTED_BY_TRANSACTIONAL_PRODUCER =
            List.of(
                    ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                    ProducerConfig.ACKS_CONFIG,
                    ProducerConfig.RETRIES_CONFIG,
                    ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION);

    private final Properties kafkaProducerConfig;
    private final boolean reusesTransactionalIds;
    private final BiFunction<Properties, String, FlinkKafkaInternalProducer<?, ?>> producerFactory;

    public RestoredTransactionCommitter(
            Properties kafkaProducerConfig,
            boolean reusesTransactionalIds,
            BiFunction<Properties, String, FlinkKafkaInternalProducer<?, ?>> producerFactory) {
        this.kafkaProducerConfig = forTransactionalProducer(kafkaProducerConfig);
        this.reusesTransactionalIds = reusesTransactionalIds;
        this.producerFactory = producerFactory;
    }

    private static Properties forTransactionalProducer(Properties kafkaProducerConfig) {
        final Properties properties = new Properties();
        properties.putAll(kafkaProducerConfig);
        SETTINGS_REJECTED_BY_TRANSACTIONAL_PRODUCER.forEach(properties::remove);
        return properties;
    }

    @Override
    public void commit(Collection<CommitRequest<KafkaCommittable>> requests)
            throws IOException, InterruptedException {
        if (requests.isEmpty()) {
            return;
        }
        LOG.info(
                "Committing {} transaction(s) restored from a snapshot of an execution with {}.",
                requests.size(),
                DeliveryGuarantee.EXACTLY_ONCE);
        final List<CommitRequest<KafkaCommittable>> nonFatalRequests =
                new ArrayList<>(requests.size());
        for (CommitRequest<KafkaCommittable> request : requests) {
            nonFatalRequests.add(new NonFatalRequest(request));
        }
        // Restored transactions arrive only at the start of the task. The producer that commits
        // them has no use afterwards, so it is not kept beyond this call.
        try (KafkaCommitter committer =
                new KafkaCommitter(
                        kafkaProducerConfig,
                        reusesTransactionalIds,
                        producerFactory,
                        new NoBackchannel())) {
            committer.commit(nonFatalRequests);
        }
    }

    @Override
    public void close() {}

    /**
     * Forwards to the wrapped request, except that a failure for an unknown reason is not fatal.
     */
    private static final class NonFatalRequest implements CommitRequest<KafkaCommittable> {
        private final CommitRequest<KafkaCommittable> request;

        private NonFatalRequest(CommitRequest<KafkaCommittable> request) {
            this.request = request;
        }

        @Override
        public KafkaCommittable getCommittable() {
            return request.getCommittable();
        }

        @Override
        public int getNumberOfRetries() {
            return request.getNumberOfRetries();
        }

        @Override
        public void signalFailedWithKnownReason(Throwable t) {
            request.signalFailedWithKnownReason(t);
        }

        @Override
        public void signalFailedWithUnknownReason(Throwable t) {
            request.signalFailedWithKnownReason(t);
        }

        @Override
        public void retryLater() {
            request.retryLater();
        }

        @Override
        public void updateAndRetryLater(KafkaCommittable committable) {
            request.updateAndRetryLater(committable);
        }

        @Override
        public void signalAlreadyCommitted() {
            request.signalAlreadyCommitted();
        }

        @Override
        public String toString() {
            return request.toString();
        }
    }

    /** A writer without transactions has no producers to recycle, so nothing listens. */
    private static final class NoBackchannel implements WritableBackchannel<TransactionFinished> {
        @Override
        public void send(TransactionFinished message) {}

        @Override
        public boolean isEstablished() {
            return false;
        }

        @Override
        public void close() {}
    }
}
