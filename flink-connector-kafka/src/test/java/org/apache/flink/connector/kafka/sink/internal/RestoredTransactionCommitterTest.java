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

import org.apache.flink.api.connector.sink2.mocks.MockCommitRequest;
import org.apache.flink.connector.kafka.sink.KafkaCommittable;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.InvalidPidMappingException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static org.apache.flink.connector.kafka.testutils.KafkaUtil.checkProducerLeak;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RestoredTransactionCommitter}. */
class RestoredTransactionCommitterTest {

    private static final long PRODUCER_ID = 0;
    private static final short EPOCH = 0;
    private static final String TRANSACTIONAL_ID = "transactionalId";

    private final List<FlinkKafkaInternalProducer<?, ?>> createdProducers = new ArrayList<>();
    private final List<String> committedTransactionalIds = new ArrayList<>();

    @AfterEach
    void check() {
        checkProducerLeak();
    }

    @Test
    void testCommitRestoredTransaction() throws Exception {
        final RecordingCommitRequest request = restoredTransaction();
        try (RestoredTransactionCommitter committer =
                new RestoredTransactionCommitter(getProperties(), false, producerFactory(null))) {
            committer.commit(Collections.singletonList(request));

            assertThat(committedTransactionalIds).containsExactly(TRANSACTIONAL_ID);
            assertThat(request.getNumberOfRetries()).isZero();
            assertThat(request.getFailedWithKnownReason()).isNull();
            assertThat(request.getFailedWithUnknownReason()).isNull();
            // the producer is not kept for the rest of the task
            assertThat(createdProducers).hasSize(1).allMatch(FlinkKafkaInternalProducer::isClosed);
        }
    }

    @Test
    void testNoProducerWithoutRestoredTransactions() throws Exception {
        try (RestoredTransactionCommitter committer =
                new RestoredTransactionCommitter(getProperties(), false, producerFactory(null))) {
            committer.commit(Collections.emptyList());
        }

        assertThat(createdProducers).isEmpty();
    }

    /**
     * Producer settings that the writer of a sink without transactions accepts and that a
     * transactional producer rejects.
     */
    static Stream<Map<String, String>> settingsOfNonTransactionalSink() {
        return Stream.of(
                Map.of(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false"),
                Map.of(ProducerConfig.ACKS_CONFIG, "1"),
                Map.of(ProducerConfig.RETRIES_CONFIG, "0"),
                // the client allows more than 5 requests in flight only without idempotence
                Map.of(
                        ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                        "false",
                        ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
                        "6"));
    }

    @ParameterizedTest
    @MethodSource("settingsOfNonTransactionalSink")
    void testCommitWithSettingsOfNonTransactionalSink(Map<String, String> settings)
            throws Exception {
        final Properties properties = getProperties();
        properties.putAll(settings);
        // what the writer of the sink creates from these settings
        new FlinkKafkaInternalProducer<byte[], byte[]>(properties).close();

        final RecordingCommitRequest request = restoredTransaction();
        try (RestoredTransactionCommitter committer =
                new RestoredTransactionCommitter(properties, false, producerFactory(null))) {
            committer.commit(Collections.singletonList(request));
        }

        assertThat(committedTransactionalIds).containsExactly(TRANSACTIONAL_ID);
    }

    @Test
    void testRetryOnRetriableErrorWithNewProducer() throws Exception {
        final RecordingCommitRequest request = restoredTransaction();
        final BiFunction<Properties, String, FlinkKafkaInternalProducer<?, ?>> failing =
                producerFactory(new TimeoutException("test"));
        final BiFunction<Properties, String, FlinkKafkaInternalProducer<?, ?>> succeeding =
                producerFactory(null);
        try (RestoredTransactionCommitter committer =
                new RestoredTransactionCommitter(
                        getProperties(),
                        false,
                        // only the first attempt fails
                        (properties, transactionalId) ->
                                (createdProducers.isEmpty() ? failing : succeeding)
                                        .apply(properties, transactionalId))) {
            committer.commit(Collections.singletonList(request));

            assertThat(request.getNumberOfRetries()).isOne();
            assertThat(committedTransactionalIds).isEmpty();
            assertThat(createdProducers).hasSize(1).allMatch(FlinkKafkaInternalProducer::isClosed);

            committer.commit(Collections.singletonList(request));

            assertThat(request.getNumberOfRetries()).isOne();
            assertThat(committedTransactionalIds).containsExactly(TRANSACTIONAL_ID);
            assertThat(createdProducers).hasSize(2).allMatch(FlinkKafkaInternalProducer::isClosed);
        }

        assertThat(request.getFailedWithKnownReason()).isNull();
        assertThat(request.getFailedWithUnknownReason()).isNull();
    }

    @Test
    void testKnownFatalErrorDoesNotFailJob() throws Exception {
        final ProducerFencedException failure = new ProducerFencedException("test");
        final RecordingCommitRequest request = restoredTransaction();
        try (RestoredTransactionCommitter committer =
                new RestoredTransactionCommitter(
                        getProperties(), false, producerFactory(failure))) {
            committer.commit(Collections.singletonList(request));

            assertThat(createdProducers).hasSize(1).allMatch(FlinkKafkaInternalProducer::isClosed);
        }

        assertThat(request.getFailedWithKnownReason()).isSameAs(failure);
        assertThat(request.getFailedWithUnknownReason()).isNull();
    }

    /** {@link KafkaCommitter} alone reports this failure in the way that fails the job. */
    @Test
    void testUnknownFatalErrorDoesNotFailJob() throws Exception {
        final InvalidPidMappingException failure = new InvalidPidMappingException("test");
        final RecordingCommitRequest request = restoredTransaction();
        try (RestoredTransactionCommitter committer =
                new RestoredTransactionCommitter(
                        getProperties(), false, producerFactory(failure))) {
            committer.commit(Collections.singletonList(request));

            assertThat(createdProducers).hasSize(1).allMatch(FlinkKafkaInternalProducer::isClosed);
        }

        assertThat(request.getFailedWithKnownReason()).isSameAs(failure);
        assertThat(request.getFailedWithUnknownReason()).isNull();
        assertThat(request.getNumberOfRetries()).isZero();
    }

    /** A committable as it comes out of the state: without the producer of the writer. */
    private static RecordingCommitRequest restoredTransaction() {
        return new RecordingCommitRequest(
                new KafkaCommittable(PRODUCER_ID, EPOCH, TRANSACTIONAL_ID, null));
    }

    private BiFunction<Properties, String, FlinkKafkaInternalProducer<?, ?>> producerFactory(
            @Nullable RuntimeException commitException) {
        return (properties, transactionalId) -> {
            final FlinkKafkaInternalProducer<byte[], byte[]> producer =
                    new FlinkKafkaInternalProducer<>(properties, transactionalId) {
                        @Override
                        public void commitTransaction() {
                            if (commitException != null) {
                                throw commitException;
                            }
                            committedTransactionalIds.add(getTransactionalId());
                        }
                    };
            createdProducers.add(producer);
            return producer;
        };
    }

    private static Properties getProperties() {
        final Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:1");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return properties;
    }

    private static class RecordingCommitRequest extends MockCommitRequest<KafkaCommittable> {
        @Nullable private Throwable failedWithKnownReason;

        private RecordingCommitRequest(KafkaCommittable committable) {
            super(committable);
        }

        @Override
        public void signalFailedWithKnownReason(Throwable t) {
            failedWithKnownReason = t;
        }

        @Nullable
        private Throwable getFailedWithKnownReason() {
            return failedWithKnownReason;
        }
    }
}
