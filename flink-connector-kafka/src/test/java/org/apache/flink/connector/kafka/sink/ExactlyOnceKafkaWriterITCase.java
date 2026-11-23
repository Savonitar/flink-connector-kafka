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

import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.connector.sink2.mocks.MockCommitRequest;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.internal.CheckpointTransaction;
import org.apache.flink.connector.kafka.sink.internal.FlinkKafkaInternalProducer;
import org.apache.flink.connector.kafka.sink.internal.KafkaCommitter;
import org.apache.flink.connector.kafka.sink.internal.ProducerPoolImpl;
import org.apache.flink.connector.kafka.sink.internal.TransactionFinished;
import org.apache.flink.connector.kafka.sink.internal.TransactionOwnership;
import org.apache.flink.connector.kafka.sink.internal.WritableBackchannel;
import org.apache.flink.connector.kafka.util.AdminUtils;
import org.apache.flink.connector.testutils.source.TestingTaskInfo;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.ExceptionUtils;

import com.google.common.collect.Iterables;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TransactionListing;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RetriableException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.apache.flink.connector.kafka.sink.internal.TransactionalIdFactory.buildAttemptScopedPrefix;
import static org.apache.flink.connector.kafka.sink.internal.TransactionalIdFactory.buildTransactionalId;
import static org.apache.flink.connector.kafka.sink.internal.TransactionalIdFactory.extractPrefix;
import static org.apache.flink.connector.kafka.testutils.KafkaUtil.drainAllRecordsFromTopic;
import static org.apache.flink.connector.kafka.testutils.KafkaUtil.waitUntilNoTransactionIsOpen;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
    @Test
    void shouldAbortLingeringTransactions() throws Exception {
        try (final ExactlyOnceKafkaWriter<Integer> failedWriter =
                createWriter(DeliveryGuarantee.EXACTLY_ONCE)) {

            // create two lingering transactions
            onCheckpointBarrier(failedWriter, 1);
            onCheckpointBarrier(failedWriter, 2);

            // use state to ensure that the new writer knows about the prefix of the old attempt
            KafkaWriterState state =
                    new KafkaWriterState(
                            attemptScopedPrefix(failedWriter),
                            0,
                            1,
                            TransactionOwnership.IMPLICIT_BY_SUBTASK_ID,
                            List.of());

            try (final KafkaWriter<Integer> recoveredWriter =
                    restoreWriter(EXACTLY_ONCE, List.of(state), createInitContext())) {
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
    @ValueSource(ints = {1, 2, 3})
    void shouldNotAbortPrecommittedTransactions(int numCheckpointed) throws Exception {
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
                            this::withPooling,
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

    /**
     * Tests that a writer of an earlier attempt does not end the transaction that the writer of a
     * later attempt of the same subtask has pre-committed, when the earlier writer is created only
     * after the pre-commit. The earlier writer may start or refuse to start; the test accepts both.
     */
    @ParameterizedTest
    @EnumSource(TransactionNamingStrategy.class)
    void shouldNotAbortPrecommittedTransactionOfLaterAttempt(
            TransactionNamingStrategy namingStrategy) throws Exception {
        // both attempts run the same sink, so they share the prefix and the subtask id
        final String prefix = getTransactionalPrefix();
        final Consumer<KafkaSinkBuilder<?>> sink =
                builder ->
                        builder.setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                                .setTransactionNamingStrategy(namingStrategy)
                                .setTransactionalIdPrefix(prefix);
        final int earlierAttempt = 0;
        final int laterAttempt = 1;

        try (final ExactlyOnceKafkaWriter<Integer> laterWriter =
                        createWriter(sink, createInitContext(laterAttempt));
                final KafkaCommitter committer =
                        new KafkaCommitter(
                                getKafkaClientConfiguration(),
                                prefix,
                                0,
                                laterAttempt,
                                namingStrategy == TransactionNamingStrategy.POOLING,
                                FlinkKafkaInternalProducer::new)) {
            final KafkaCommittable precommitted = onCheckpointBarrier(laterWriter, 1).f1;

            // the earlier attempt is still alive and gets to create its writer only now
            ExactlyOnceKafkaWriter<Integer> earlierWriter = null;
            try {
                try {
                    earlierWriter = createWriter(sink, createInitContext(earlierAttempt));
                } catch (Exception e) {
                    // A writer that notices the later attempt and does not start is fine as well.
                    // A broker that did not answer in time is not such a refusal: the transaction
                    // of the later attempt would survive, and the test would pass for no reason.
                    if (ExceptionUtils.findThrowable(e, RetriableException.class).isPresent()
                            || ExceptionUtils.findThrowable(e, InterruptException.class)
                                    .isPresent()) {
                        throw e;
                    }
                    LOG.info("The writer of the earlier attempt did not start.", e);
                }
                committer.commit(List.of(new MockCommitRequest<>(precommitted)));
            } finally {
                if (earlierWriter != null) {
                    earlierWriter.close();
                }
            }

            // the broker ends a transaction in the partition after it has answered the producer
            waitUntilNoTransactionIsOpen(topic, getKafkaClientConfiguration());
            assertThat(drainAllRecordsFromTopic(topic, getKafkaClientConfiguration(), true))
                    .as(
                            "committed records; without the record, the writer of the earlier "
                                    + "attempt ended the transaction that the later attempt had "
                                    + "pre-committed")
                    .hasSize(1);
        }
    }

    /**
     * Tests that a writer names its transactions and its state after its execution attempt, so that
     * no other attempt of the subtask uses the same transactional ids.
     */
    @ParameterizedTest
    @EnumSource(TransactionNamingStrategy.class)
    void shouldScopeTransactionalIdsToAttempt(TransactionNamingStrategy namingStrategy)
            throws Exception {
        final String prefix = getTransactionalPrefix();
        final int attempt = 3;
        final long restoredCheckpointId = 7;
        final SinkInitContext initContext = createInitContext(attempt);
        initContext.setRestoredCheckpointId(restoredCheckpointId);

        try (final ExactlyOnceKafkaWriter<Integer> writer =
                createWriter(exactlyOnce(namingStrategy, prefix), initContext)) {
            final String scopedPrefix =
                    buildAttemptScopedPrefix(prefix, restoredCheckpointId, 0, attempt);
            assertThat(extractPrefix(writer.getCurrentProducer().getTransactionalId()))
                    .isEqualTo(scopedPrefix);

            final Tuple2<KafkaWriterState, KafkaCommittable> checkpoint =
                    onCheckpointBarrier(writer, (int) restoredCheckpointId + 1);
            assertThat(extractPrefix(checkpoint.f1.getTransactionalId())).isEqualTo(scopedPrefix);
            // the writer that restores the state finds the transactions of this attempt with it
            assertThat(checkpoint.f0.getTransactionalIdPrefix()).isEqualTo(scopedPrefix);
            assertThat(extractPrefix(writer.getCurrentProducer().getTransactionalId()))
                    .isEqualTo(scopedPrefix);
        }
    }

    /**
     * Tests that a writer aborts the transaction that an earlier attempt of its subtask has left
     * open, when both attempts started from the same point and no state tells of the earlier one.
     */
    @ParameterizedTest
    @EnumSource(TransactionNamingStrategy.class)
    void shouldAbortTransactionsOfEarlierAttempt(TransactionNamingStrategy namingStrategy)
            throws Exception {
        final Consumer<KafkaSinkBuilder<?>> sink =
                exactlyOnce(namingStrategy, getTransactionalPrefix());

        try (final ExactlyOnceKafkaWriter<Integer> earlierWriter =
                createWriter(sink, createInitContext(0))) {
            // the checkpoint does not complete, so the transaction stays open
            onCheckpointBarrier(earlierWriter, 1);
        }
        try (AdminClient admin = AdminClient.create(getKafkaClientConfiguration())) {
            assertThat(AdminUtils.getOpenTransactionsForTopics(admin, Collections.singleton(topic)))
                    .hasSize(1);
        }

        try (final ExactlyOnceKafkaWriter<Integer> laterWriter =
                createWriter(sink, createInitContext(1))) {
            waitUntilNoTransactionIsOpen(topic, getKafkaClientConfiguration());
            assertThat(drainAllRecordsFromTopic(topic, getKafkaClientConfiguration(), true))
                    .isEmpty();
        }
    }

    /**
     * Tests that a writer aborts the transactions of a recovered state that was written before
     * transactional ids were scoped to an attempt. Such a state holds the prefix of the sink, and
     * its transactions are named {@code prefix-subtask-offset}.
     */
    @ParameterizedTest
    @EnumSource(TransactionNamingStrategy.class)
    void shouldAbortTransactionsOfPrefixWithoutAttemptScope(
            TransactionNamingStrategy namingStrategy) throws Exception {
        final String prefix = getTransactionalPrefix();
        try (FlinkKafkaInternalProducer<byte[], byte[]> producer =
                new FlinkKafkaInternalProducer<>(
                        getKafkaClientConfiguration(), buildTransactionalId(prefix, 0, 1))) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(topic, "1".getBytes()));
            producer.flush();
            // closing the producer leaves the transaction open
        }
        final KafkaWriterState state =
                new KafkaWriterState(
                        prefix,
                        0,
                        1,
                        namingStrategy == TransactionNamingStrategy.POOLING
                                ? TransactionOwnership.EXPLICIT_BY_WRITER_STATE
                                : TransactionOwnership.IMPLICIT_BY_SUBTASK_ID,
                        List.of());

        try (final ExactlyOnceKafkaWriter<Integer> writer =
                restoreWriter(
                        exactlyOnce(namingStrategy, prefix), List.of(state), createInitContext())) {
            waitUntilNoTransactionIsOpen(topic, getKafkaClientConfiguration());
            assertThat(drainAllRecordsFromTopic(topic, getKafkaClientConfiguration(), true))
                    .isEmpty();
        }
    }

    /**
     * Tests that a restored writer aborts the transaction under the id of a recovered transaction
     * once the committer reports that it could not commit that transaction. With pooled ids, the
     * attempt that wrote the state may have used the id again for a transaction that no checkpoint
     * covers. The cleanup at the start of the writer spares the id for the committer.
     */
    @Test
    void shouldAbortIdOfRecoveredTransactionAfterFailedCommit() throws Exception {
        final String prefix = getTransactionalPrefix();
        final String prefixOfEarlierAttempt = buildAttemptScopedPrefix(prefix, 0, 0, 0);
        final String reusedId = buildTransactionalId(prefixOfEarlierAttempt, 0, 0);
        try (FlinkKafkaInternalProducer<byte[], byte[]> producer =
                new FlinkKafkaInternalProducer<>(getKafkaClientConfiguration(), reusedId)) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(topic, "1".getBytes()));
            producer.flush();
            // closing the producer leaves the transaction open
        }
        final KafkaWriterState state =
                new KafkaWriterState(
                        prefixOfEarlierAttempt,
                        0,
                        1,
                        TransactionOwnership.EXPLICIT_BY_WRITER_STATE,
                        List.of(new CheckpointTransaction(reusedId, 1)));
        final SinkInitContext initContext = createInitContext();
        initContext.setRestoredCheckpointId(1);

        try (final ExactlyOnceKafkaWriter<Integer> writer =
                        restoreWriter(this::withPooling, List.of(state), initContext);
                WritableBackchannel<TransactionFinished> backchannel = getBackchannel(writer);
                AdminClient admin = AdminClient.create(getKafkaClientConfiguration())) {
            assertThat(AdminUtils.getOpenTransactionsForTopics(admin, Collections.singleton(topic)))
                    .extracting(TransactionListing::transactionalId)
                    .containsExactly(reusedId);

            backchannel.send(TransactionFinished.erroneously(reusedId));
            writer.snapshotState(2);

            waitUntilNoTransactionIsOpen(topic, getKafkaClientConfiguration());
            assertThat(drainAllRecordsFromTopic(topic, getKafkaClientConfiguration(), true))
                    .isEmpty();
        }
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

    @Test
    void testPoolingClosesAdminClient() throws Exception {
        final ExactlyOnceKafkaWriter<Integer> writer =
                createExactlyOnceWriter(TransactionNamingStrategy.POOLING);
        // The outer try provides fallback cleanup so a failing test does not leak the client.
        // The closure assertion runs before that fallback cleanup.
        try (final Admin adminClient = writer.getAdminClient()) {
            try (writer) {
                assertThat(adminClient)
                        .as("POOLING lists transactions through an admin client")
                        .isNotNull();
                assertThat(adminClient.listTopics().names().get(10, TimeUnit.SECONDS))
                        .contains(topic);
            }

            assertThatThrownBy(() -> adminClient.listTopics().names().get(10, TimeUnit.SECONDS))
                    .as("The writer must close its admin client")
                    .hasRootCauseExactlyInstanceOf(IllegalStateException.class)
                    .hasRootCauseMessage("Cannot accept new calls when AdminClient is closing.");
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = TransactionNamingStrategy.class,
            names = "POOLING",
            mode = EnumSource.Mode.EXCLUDE)
    void testNonPoolingStrategiesNeverCreateAdminClient(TransactionNamingStrategy namingStrategy)
            throws Exception {
        final ExactlyOnceKafkaWriter<Integer> writer = createExactlyOnceWriter(namingStrategy);
        try (writer) {
            assertThat(writer.getAdminClient())
                    .as("Naming strategy %s should not create an admin client", namingStrategy)
                    .isNull();
        }
        assertThat(writer.getAdminClient())
                .as("Closing the writer must not create an admin client")
                .isNull();
    }

    /**
     * Test that a pooled id is skipped while its transaction is ongoing and used again when the
     * committer has finished the transaction. The transactions of the recovered state have the
     * prefix of the attempt that wrote them, so they do not take ids from the restored writer.
     */
    @Test
    void shouldSkipIdsOfOngoingPooledTransactions() throws Exception {
        String prefix = getTransactionalPrefix();
        String prefixOfEarlierAttempt = buildAttemptScopedPrefix(prefix, 1, 0, 0);
        CheckpointTransaction t1 =
                new CheckpointTransaction(buildTransactionalId(prefixOfEarlierAttempt, 0, 0), 2);
        CheckpointTransaction t2 =
                new CheckpointTransaction(buildTransactionalId(prefixOfEarlierAttempt, 0, 1), 4);
        final KafkaWriterState writerState =
                new KafkaWriterState(
                        prefixOfEarlierAttempt,
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
            String ownPrefix = attemptScopedPrefix(writer);
            assertThat(ownPrefix).isNotEqualTo(prefixOfEarlierAttempt);
            // no transaction is finished, so every checkpoint takes the next offset
            for (int expectedOffset : new int[] {0, 1, 2}) {
                assertThat(writer.getCurrentProducer().getTransactionalId())
                        .isEqualTo(buildTransactionalId(ownPrefix, 0, expectedOffset));
                writer.write(checkpointId, SINK_WRITER_CONTEXT);
                writer.flush(false);
                writer.prepareCommit();
                writer.snapshotState(++checkpointId);
            }

            // free 1, which also frees 0 and the recovered transactions; 0 is returned first
            backchannel.send(new TransactionFinished(buildTransactionalId(ownPrefix, 0, 1), true));
            writer.write(checkpointId, SINK_WRITER_CONTEXT);
            writer.prepareCommit();
            writer.snapshotState(++checkpointId);
            assertThat(writer.getCurrentProducer().getTransactionalId())
                    .isEqualTo(buildTransactionalId(ownPrefix, 0, 0));

            writer.write(checkpointId, SINK_WRITER_CONTEXT);
            writer.prepareCommit();
            writer.snapshotState(++checkpointId);
            assertThat(writer.getCurrentProducer().getTransactionalId())
                    .isEqualTo(buildTransactionalId(ownPrefix, 0, 1));
        }
    }

    private ExactlyOnceKafkaWriter<Integer> createExactlyOnceWriter(
            TransactionNamingStrategy namingStrategy) throws IOException {
        return createWriter(
                builder ->
                        builder.setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                                .setTransactionNamingStrategy(namingStrategy),
                createInitContext());
    }

    /** The prefix under which the attempt of the writer names its transactions. */
    private static String attemptScopedPrefix(KafkaWriter<?> writer) {
        return extractPrefix(writer.getCurrentProducer().getTransactionalId());
    }

    private static Consumer<KafkaSinkBuilder<?>> exactlyOnce(
            TransactionNamingStrategy namingStrategy, String transactionalIdPrefix) {
        return builder ->
                builder.setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                        .setTransactionNamingStrategy(namingStrategy)
                        .setTransactionalIdPrefix(transactionalIdPrefix);
    }

    private SinkInitContext createInitContext(int attemptNumber) {
        final TaskInfo taskInfo =
                new TestingTaskInfo.Builder()
                        .setTaskName("TestTask")
                        .setMaxNumberOfParallelSubtasks(1)
                        .setIndexOfThisSubtask(0)
                        .setAttemptNumber(attemptNumber)
                        .setNumberOfParallelSubtasks(1)
                        .build();
        return new SinkInitContext(createSinkWriterMetricGroup(), timeService, null) {
            @Override
            public TaskInfo getTaskInfo() {
                return taskInfo;
            }
        };
    }

    private static Collection<FlinkKafkaInternalProducer<byte[], byte[]>> getProducers(
            ExactlyOnceKafkaWriter<Integer> writer) {
        return ((ProducerPoolImpl) writer.getProducerPool()).getProducers();
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
}
