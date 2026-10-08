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

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.internal.CheckpointTransaction;
import org.apache.flink.connector.kafka.sink.internal.FlinkKafkaInternalProducer;
import org.apache.flink.connector.kafka.sink.internal.ProducerPoolImpl;
import org.apache.flink.connector.kafka.sink.internal.TransactionFinished;
import org.apache.flink.connector.kafka.sink.internal.TransactionOwnership;
import org.apache.flink.connector.kafka.sink.internal.WritableBackchannel;
import org.apache.flink.connector.kafka.util.AdminUtils;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.MiniClusterExtension;

import com.google.common.collect.Iterables;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.connector.kafka.sink.internal.TransactionalIdFactory.buildTransactionalId;
import static org.apache.flink.connector.kafka.testutils.KafkaUtil.drainAllRecordsFromTopic;
import static org.apache.flink.core.testutils.CommonTestUtils.waitUtil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatCode;

/** Tests for the standalone KafkaWriter. */
public class ExactlyOnceKafkaWriterITCase extends KafkaWriterTestBase {

    @RegisterExtension
    public static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(2)
                            .setNumberSlotsPerTaskManager(8)
                            .setConfiguration(new Configuration())
                            .build());

    private static final Consumer<KafkaSinkBuilder<?>> EXACTLY_ONCE =
            sink -> sink.setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE);

    @Test
    void testFlushAsyncErrorPropagationAndErrorCounter() throws Exception {
        Properties properties = getKafkaClientConfiguration();

        final SinkWriterMetricGroup metricGroup = createSinkWriterMetricGroup();

        final KafkaWriter<Integer> writer =
                createWriter(
                        DeliveryGuarantee.EXACTLY_ONCE,
                        new SinkInitContext(metricGroup, timeService, null));
        final Counter numRecordsOutErrors = metricGroup.getNumRecordsOutErrorsCounter();
        assertThat(numRecordsOutErrors.getCount()).isEqualTo(0L);

        triggerProducerException(writer, properties);

        // test flush
        assertThatCode(() -> writer.flush(false))
                .hasRootCauseExactlyInstanceOf(ProducerFencedException.class);
        assertThat(numRecordsOutErrors.getCount()).isEqualTo(1L);

        assertThatCode(() -> writer.write(1, SINK_WRITER_CONTEXT))
                .as("the exception is not thrown again")
                .doesNotThrowAnyException();
        assertThat(numRecordsOutErrors.getCount()).isEqualTo(1L);

        // async exception is checked and thrown on close
        assertThatCode(writer::close).hasRootCauseInstanceOf(ProducerFencedException.class);
    }

    @Test
    void testWriteAsyncErrorPropagationAndErrorCounter() throws Exception {
        Properties properties = getKafkaClientConfiguration();

        final SinkWriterMetricGroup metricGroup = createSinkWriterMetricGroup();

        final KafkaWriter<Integer> writer =
                createWriter(
                        DeliveryGuarantee.EXACTLY_ONCE,
                        new SinkInitContext(metricGroup, timeService, null));
        final Counter numRecordsOutErrors = metricGroup.getNumRecordsOutErrorsCounter();
        assertThat(numRecordsOutErrors.getCount()).isEqualTo(0L);

        triggerProducerException(writer, properties);
        // to ensure that the exceptional send request has completed
        writer.getCurrentProducer().flush();

        assertThatCode(() -> writer.write(1, SINK_WRITER_CONTEXT))
                .hasRootCauseExactlyInstanceOf(ProducerFencedException.class);
        assertThat(numRecordsOutErrors.getCount()).isEqualTo(1L);

        assertThatCode(() -> writer.write(1, SINK_WRITER_CONTEXT))
                .as("the exception is not thrown again")
                .doesNotThrowAnyException();
        assertThat(numRecordsOutErrors.getCount()).isEqualTo(1L);

        // async exception is checked and thrown on close
        assertThatCode(writer::close).hasRootCauseInstanceOf(ProducerFencedException.class);
    }

    @Test
    void testMailboxAsyncErrorPropagationAndErrorCounter() throws Exception {
        Properties properties = getKafkaClientConfiguration();

        SinkInitContext sinkInitContext =
                new SinkInitContext(createSinkWriterMetricGroup(), timeService, null);

        final KafkaWriter<Integer> writer =
                createWriter(DeliveryGuarantee.EXACTLY_ONCE, sinkInitContext);
        final Counter numRecordsOutErrors =
                sinkInitContext.metricGroup.getNumRecordsOutErrorsCounter();
        assertThat(numRecordsOutErrors.getCount()).isEqualTo(0L);

        triggerProducerException(writer, properties);
        // to ensure that the exceptional send request has completed
        writer.getCurrentProducer().flush();

        assertThatCode(
                        () -> {
                            while (sinkInitContext.getMailboxExecutor().tryYield()) {
                                // execute all mails
                            }
                        })
                .hasRootCauseExactlyInstanceOf(ProducerFencedException.class);

        assertThat(numRecordsOutErrors.getCount()).isEqualTo(1L);

        assertThatCode(() -> writer.write(1, SINK_WRITER_CONTEXT))
                .as("the exception is not thrown again")
                .doesNotThrowAnyException();
        assertThat(numRecordsOutErrors.getCount()).isEqualTo(1L);

        // async exception is checked and thrown on close
        assertThatCode(writer::close).hasRootCauseInstanceOf(ProducerFencedException.class);
    }

    @Test
    void testCloseAsyncErrorPropagationAndErrorCounter() throws Exception {
        Properties properties = getKafkaClientConfiguration();

        final SinkWriterMetricGroup metricGroup = createSinkWriterMetricGroup();

        final KafkaWriter<Integer> writer =
                createWriter(
                        DeliveryGuarantee.EXACTLY_ONCE,
                        new SinkInitContext(metricGroup, timeService, null));
        final Counter numRecordsOutErrors = metricGroup.getNumRecordsOutErrorsCounter();
        assertThat(numRecordsOutErrors.getCount()).isEqualTo(0L);

        triggerProducerException(writer, properties);
        // to ensure that the exceptional send request has completed
        writer.getCurrentProducer().flush();

        // test flush
        assertThatCode(writer::close)
                .as("flush should throw the exception from the WriterCallback")
                .hasRootCauseExactlyInstanceOf(ProducerFencedException.class);
        assertThat(numRecordsOutErrors.getCount()).isEqualTo(1L);
    }

    private void triggerProducerException(KafkaWriter<Integer> writer, Properties properties)
            throws IOException {
        final String transactionalId = writer.getCurrentProducer().getTransactionalId();

        try (FlinkKafkaInternalProducer<byte[], byte[]> producer =
                new FlinkKafkaInternalProducer<>(properties, transactionalId)) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(topic, "1".getBytes()));
            producer.commitTransaction();
        }

        writer.write(1, SINK_WRITER_CONTEXT);
    }

    /** Test that producer is not accidentally recreated or pool is used. */
    @ParameterizedTest
    @EnumSource(TransactionAbortMethod.class)
    void shouldAbortLingeringTransactions(TransactionAbortMethod abortMethod) throws Exception {
        try (final ExactlyOnceKafkaWriter<Integer> failedWriter =
                createWriter(DeliveryGuarantee.EXACTLY_ONCE)) {

            // create two lingering transactions
            onCheckpointBarrier(failedWriter, 1);
            onCheckpointBarrier(failedWriter, 2);

            // use state to ensure that the new writer knows about the old prefix
            KafkaWriterState state =
                    new KafkaWriterState(
                            failedWriter.getTransactionalIdPrefix(),
                            0,
                            1,
                            TransactionOwnership.IMPLICIT_BY_SUBTASK_ID,
                            List.of());

            final String recorderId = UUID.randomUUID().toString();
            try (final KafkaWriter<Integer> recoveredWriter =
                    restoreWriter(
                            EXACTLY_ONCE.andThen(
                                    builder ->
                                            builder.setTransactionAbortMethod(abortMethod)
                                                    .setProperty(
                                                            ProducerConfig
                                                                    .INTERCEPTOR_CLASSES_CONFIG,
                                                            AbortPhaseProducerRecorder.class
                                                                    .getName())
                                                    .setProperty(
                                                            AbortPhaseProducerRecorder
                                                                    .RECORDER_ID_CONFIG,
                                                            recorderId)),
                            List.of(state),
                            createInitContext())) {
                if (abortMethod == TransactionAbortMethod.ADMIN_FENCE_PRODUCERS) {
                    // the admin path aborts without constructing a producer
                    assertThat(AbortPhaseProducerRecorder.constructedWhileAborting(recorderId))
                            .isZero();
                } else {
                    assertThat(AbortPhaseProducerRecorder.constructedWhileAborting(recorderId))
                            .isPositive();
                }
                recoveredWriter.write(1, SINK_WRITER_CONTEXT);

                recoveredWriter.flush(false);
                Collection<KafkaCommittable> committables = recoveredWriter.prepareCommit();
                recoveredWriter.snapshotState(1);
                assertThat(committables).hasSize(1);
                final KafkaCommittable committable = committables.stream().findFirst().get();
                assertThat(committable.getProducer().isPresent()).isTrue();

                committable.getProducer().get().commitTransaction();

                List<ConsumerRecord<byte[], byte[]>> records =
                        drainAllRecordsFromTopic(topic, getKafkaClientConfiguration(), true);
                assertThat(records).hasSize(1);
            }
        }
    }

    /** Test that writer does not abort those transactions that are passed in as writer state. */
    @ParameterizedTest
    @MethodSource("precommittedTransactionsParameters")
    void shouldNotAbortPrecommittedTransactions(
            int numCheckpointed, TransactionAbortMethod abortMethod) throws Exception {
        try (final KafkaWriter<Integer> failedWriter =
                createWriter(DeliveryGuarantee.EXACTLY_ONCE)) {

            // create three precommitted transactions
            List<KafkaWriterState> states =
                    Arrays.asList(
                            onCheckpointBarrier(failedWriter, 1).f0, // 1 transaction
                            onCheckpointBarrier(failedWriter, 2).f0, // 2 transactions
                            onCheckpointBarrier(failedWriter, 3).f0); // 3 transactions

            // assume a varying number of states that have been checkpointed
            try (final ExactlyOnceKafkaWriter<Integer> recoveredWriter =
                    restoreWriter(
                            builder -> withPooling(builder).setTransactionAbortMethod(abortMethod),
                            List.of(states.get(numCheckpointed - 1)),
                            createInitContext())) {
                // test abort of recoveredWriter; this should abort all transactions that have
                // not been in the part of the checkpoint
                try (AdminClient admin = AdminClient.create(getKafkaClientConfiguration())) {
                    assertThat(
                                    AdminUtils.getOpenTransactionsForTopics(
                                            admin, Collections.singleton(topic)))
                            .hasSize(numCheckpointed);
                }
            }
        }
    }

    static Stream<Arguments> precommittedTransactionsParameters() {
        return Stream.of(1, 2, 3)
                .flatMap(
                        numCheckpointed ->
                                Arrays.stream(TransactionAbortMethod.values())
                                        .map(method -> Arguments.of(numCheckpointed, method)));
    }

    /** Test that the writer closes the admin client of the recovery sweep once the sweep ends. */
    @ParameterizedTest
    @MethodSource("sweepsWithAdminClient")
    void shouldCloseAdminClientAfterAbortingLingeringTransactions(
            TransactionNamingStrategy namingStrategy, TransactionAbortMethod abortMethod)
            throws Exception {
        final Set<Thread> adminThreadsBefore = aliveAdminClientThreads();
        try (final ExactlyOnceKafkaWriter<Integer> writer =
                createWriter(
                        builder ->
                                builder.setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                                        .setTransactionNamingStrategy(namingStrategy)
                                        .setTransactionAbortMethod(abortMethod),
                        createInitContext())) {
            // the bounded close may return before the I/O thread has ended
            waitUtil(
                    () -> adminThreadsBefore.containsAll(aliveAdminClientThreads()),
                    Duration.ofSeconds(30),
                    "The admin client of the recovery sweep is still running.");
        }
    }

    static Stream<Arguments> sweepsWithAdminClient() {
        // probing through producers is the only sweep without an admin client
        return Stream.of(
                Arguments.of(
                        TransactionNamingStrategy.INCREMENTING,
                        TransactionAbortMethod.ADMIN_FENCE_PRODUCERS),
                Arguments.of(
                        TransactionNamingStrategy.POOLING,
                        TransactionAbortMethod.PRODUCER_INIT_TRANSACTIONS),
                Arguments.of(
                        TransactionNamingStrategy.POOLING,
                        TransactionAbortMethod.ADMIN_FENCE_PRODUCERS));
    }

    /** Test that producers are reused when committed. */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void usePooledProducerForTransactional(boolean successfulTransaction) throws Exception {
        try (final ExactlyOnceKafkaWriter<Integer> writer =
                createWriter(DeliveryGuarantee.EXACTLY_ONCE)) {
            assertThat(getProducers(writer)).hasSize(0);

            writer.write(1, SINK_WRITER_CONTEXT);
            writer.flush(false);
            Collection<KafkaCommittable> committables0 = writer.prepareCommit();
            writer.snapshotState(1);
            assertThat(committables0).hasSize(1);
            final KafkaCommittable committable = committables0.stream().findFirst().get();
            assertThat(committable.getProducer().isPresent()).isTrue();

            FlinkKafkaInternalProducer<?, ?> firstProducer = committable.getProducer().get();
            assertThat(firstProducer != writer.getCurrentProducer())
                    .as("Expected different producer")
                    .isTrue();

            // recycle first producer, KafkaCommitter would commit it and then return it
            assertThat(getProducers(writer)).hasSize(0);
            firstProducer.commitTransaction();
            try (WritableBackchannel<TransactionFinished> backchannel = getBackchannel(writer)) {
                backchannel.send(
                        new TransactionFinished(
                                firstProducer.getTransactionalId(), successfulTransaction));
            }

            writer.write(1, SINK_WRITER_CONTEXT);
            writer.flush(false);
            Collection<KafkaCommittable> committables1 = writer.prepareCommit();
            writer.snapshotState(2);
            assertThat(committables1).hasSize(1);
            final KafkaCommittable committable1 = committables1.stream().findFirst().get();
            assertThat(committable1.getProducer().isPresent()).isTrue();

            assertThat(firstProducer == writer.getCurrentProducer())
                    .as("Expected recycled producer")
                    .isEqualTo(successfulTransaction);
        }
    }

    /**
     * Tests that if a pre-commit attempt occurs on an empty transaction, the writer should not emit
     * a KafkaCommittable, and instead immediately commit the empty transaction and recycle the
     * producer.
     */
    @Test
    void prepareCommitForEmptyTransaction() throws Exception {
        try (final ExactlyOnceKafkaWriter<Integer> writer =
                createWriter(DeliveryGuarantee.EXACTLY_ONCE)) {
            assertThat(getProducers(writer)).hasSize(0);

            // no data written to current transaction
            writer.flush(false);
            Collection<KafkaCommittable> emptyCommittables = writer.prepareCommit();

            assertThat(emptyCommittables).hasSize(0);
            assertThat(getProducers(writer)).hasSize(1);
            final FlinkKafkaInternalProducer<?, ?> recycledProducer =
                    Iterables.getFirst(getProducers(writer), null);
            assertThat(recycledProducer.isInTransaction()).isFalse();
        }
    }

    /**
     * Tests that open transactions are automatically aborted on close such that successive writes
     * succeed.
     */
    @Test
    void testAbortOnClose() throws Exception {
        Properties properties = getKafkaClientConfiguration();
        try (final KafkaWriter<Integer> writer = createWriter(DeliveryGuarantee.EXACTLY_ONCE)) {
            writer.write(1, SINK_WRITER_CONTEXT);
            assertThat(drainAllRecordsFromTopic(topic, properties, true)).hasSize(0);
        }

        try (final KafkaWriter<Integer> writer = createWriter(DeliveryGuarantee.EXACTLY_ONCE)) {
            writer.write(2, SINK_WRITER_CONTEXT);
            writer.flush(false);
            Collection<KafkaCommittable> committables = writer.prepareCommit();
            writer.snapshotState(1L);

            // manually commit here, which would only succeed if the first transaction was aborted
            assertThat(committables).hasSize(1);
            final KafkaCommittable committable = committables.stream().findFirst().get();
            String transactionalId = committable.getTransactionalId();
            try (FlinkKafkaInternalProducer<byte[], byte[]> producer =
                    new FlinkKafkaInternalProducer<>(properties, transactionalId)) {
                producer.resumeTransaction(committable.getProducerId(), committable.getEpoch());
                producer.commitTransaction();
            }

            assertThat(drainAllRecordsFromTopic(topic, properties, true)).hasSize(1);
        }
    }

    /** Test that producers are reused when committed. */
    @Test
    void shouldSkipIdsOfCommitterForPooledTransactions() throws Exception {
        String prefix = getTransactionalPrefix();
        CheckpointTransaction t1 = new CheckpointTransaction(buildTransactionalId(prefix, 0, 2), 2);
        CheckpointTransaction t2 = new CheckpointTransaction(buildTransactionalId(prefix, 0, 4), 4);
        final KafkaWriterState writerState =
                new KafkaWriterState(
                        prefix,
                        0,
                        1,
                        TransactionOwnership.EXPLICIT_BY_WRITER_STATE,
                        Arrays.asList(t1, t2));

        SinkInitContext initContext = createInitContext();
        int checkpointId = 9;
        initContext.setRestoredCheckpointId(checkpointId);
        try (final ExactlyOnceKafkaWriter<Integer> writer =
                        restoreWriter(
                                this::withPooling,
                                Collections.singletonList(writerState),
                                initContext);
                WritableBackchannel<TransactionFinished> backchannel = getBackchannel(writer)) {
            // offsets leave out the used 2 and 4
            for (int expectedOffset : new int[] {0, 1, 3, 5, 6}) {
                assertThat(writer.getCurrentProducer().getTransactionalId())
                        .isEqualTo(buildTransactionalId(prefix, 0, expectedOffset));
                writer.write(checkpointId, SINK_WRITER_CONTEXT);
                writer.flush(false);
                writer.prepareCommit();
                writer.snapshotState(++checkpointId);
            }

            // free 4, which also frees 2; 2 is returned first and then 4
            backchannel.send(new TransactionFinished(t2.getTransactionalId(), true));
            writer.write(checkpointId, SINK_WRITER_CONTEXT);
            writer.prepareCommit();
            writer.snapshotState(++checkpointId);
            assertThat(writer.getCurrentProducer().getTransactionalId())
                    .isEqualTo(t1.getTransactionalId());

            writer.write(checkpointId, SINK_WRITER_CONTEXT);
            writer.prepareCommit();
            writer.snapshotState(++checkpointId);
            assertThat(writer.getCurrentProducer().getTransactionalId())
                    .isEqualTo(t2.getTransactionalId());
        }
    }

    private static Collection<FlinkKafkaInternalProducer<byte[], byte[]>> getProducers(
            ExactlyOnceKafkaWriter<Integer> writer) {
        return ((ProducerPoolImpl) writer.getProducerPool()).getProducers();
    }

    private static Set<Thread> aliveAdminClientThreads() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(Thread::isAlive)
                .filter(thread -> thread.getName().contains("kafka-admin-client-thread"))
                .collect(Collectors.toSet());
    }

    private KafkaSinkBuilder<?> withPooling(KafkaSinkBuilder<?> builder) {
        return builder.setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionNamingStrategy(TransactionNamingStrategy.POOLING);
    }

    private Tuple2<KafkaWriterState, KafkaCommittable> onCheckpointBarrier(
            KafkaWriter<Integer> failedWriter, int checkpointId)
            throws IOException, InterruptedException {
        // constant number to force the same partition
        failedWriter.write(1, SINK_WRITER_CONTEXT);
        failedWriter.flush(false);
        KafkaCommittable committable = Iterables.getOnlyElement(failedWriter.prepareCommit());
        KafkaWriterState state = Iterables.getOnlyElement(failedWriter.snapshotState(checkpointId));
        return Tuple2.of(state, committable);
    }

    /**
     * Counts the producers constructed while the writer aborts lingering transactions, that is with
     * {@code abortLingeringTransactions} on the constructing thread's stack.
     */
    public static class AbortPhaseProducerRecorder implements ProducerInterceptor<byte[], byte[]> {
        static final String RECORDER_ID_CONFIG = "test.recorder.id";
        private static final Map<String, AtomicInteger> CONSTRUCTED_WHILE_ABORTING =
                new ConcurrentHashMap<>();

        static int constructedWhileAborting(String recorderId) {
            final AtomicInteger count = CONSTRUCTED_WHILE_ABORTING.get(recorderId);
            return count == null ? 0 : count.get();
        }

        @Override
        public void configure(Map<String, ?> configs) {
            final boolean aborting =
                    Arrays.stream(new Throwable().getStackTrace())
                            .anyMatch(
                                    frame ->
                                            frame.getMethodName()
                                                    .equals("abortLingeringTransactions"));
            if (aborting) {
                CONSTRUCTED_WHILE_ABORTING
                        .computeIfAbsent(
                                String.valueOf(configs.get(RECORDER_ID_CONFIG)),
                                id -> new AtomicInteger())
                        .incrementAndGet();
            }
        }

        @Override
        public ProducerRecord<byte[], byte[]> onSend(ProducerRecord<byte[], byte[]> record) {
            return record;
        }

        @Override
        public void onAcknowledgement(RecordMetadata metadata, Exception exception) {}

        @Override
        public void close() {}
    }
}
