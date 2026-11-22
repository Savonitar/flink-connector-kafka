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

import org.apache.flink.api.connector.sink2.mocks.MockCommitRequest;
import org.apache.flink.connector.kafka.sink.KafkaCommittable;
import org.apache.flink.testutils.logging.LoggerAuditingExtension;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.InvalidTxnStateException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.UnknownProducerIdException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static org.apache.flink.connector.kafka.testutils.KafkaUtil.checkProducerLeak;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.slf4j.event.Level.ERROR;

/** Tests for {@link KafkaCommitter}. */
class KafkaCommitterTest {

    private static final int PRODUCER_ID = 0;
    private static final short EPOCH = 0;
    private static final String TRANS_ID = "transactionalId";
    public static final int ATTEMPT = 2;
    public static final int SUB_ID = 1;
    private static final BiFunction<Properties, String, FlinkKafkaInternalProducer<?, ?>>
            MOCK_FACTORY = (properties, transactionalId) -> new MockProducer(properties, null);

    @RegisterExtension
    public final LoggerAuditingExtension errorLogger =
            new LoggerAuditingExtension(KafkaCommitter.class, ERROR);

    @AfterEach
    public void check() {
        checkProducerLeak();
    }

    /** Causes a network error by inactive broker and tests that a retry will happen. */
    @Test
    public void testRetryCommittableOnRetriableError() throws IOException, InterruptedException {
        Properties properties = getProperties();
        try (final KafkaCommitter committer =
                        new KafkaCommitter(
                                properties, TRANS_ID, SUB_ID, ATTEMPT, false, MOCK_FACTORY);
                FlinkKafkaInternalProducer<Object, Object> producer =
                        new FlinkKafkaInternalProducer<>(properties, TRANS_ID);
                ReadableBackchannel<TransactionFinished> backchannel =
                        BackchannelFactory.getInstance()
                                .getReadableBackchannel(SUB_ID, ATTEMPT, TRANS_ID)) {
            final MockCommitRequest<KafkaCommittable> request =
                    new MockCommitRequest<>(KafkaCommittable.of(producer));

            producer.resumeTransaction(PRODUCER_ID, EPOCH);
            committer.commit(Collections.singletonList(request));

            assertThat(request.getNumberOfRetries()).isEqualTo(1);
            assertThat(backchannel).doesNotHave(transactionFinished(true));
        }
    }

    @Test
    public void testFailJobOnUnknownFatalError() throws IOException, InterruptedException {
        Properties properties = getProperties();
        try (final KafkaCommitter committer =
                        new KafkaCommitter(
                                properties, TRANS_ID, SUB_ID, ATTEMPT, false, MOCK_FACTORY);
                FlinkKafkaInternalProducer<Object, Object> producer =
                        new FlinkKafkaInternalProducer<>(properties, TRANS_ID);
                ReadableBackchannel<TransactionFinished> backchannel =
                        BackchannelFactory.getInstance()
                                .getReadableBackchannel(SUB_ID, ATTEMPT, TRANS_ID)) {
            // will fail because transaction not started
            final MockCommitRequest<KafkaCommittable> request =
                    new MockCommitRequest<>(KafkaCommittable.of(producer));
            committer.commit(Collections.singletonList(request));
            assertThat(request.getFailedWithUnknownReason())
                    .isInstanceOf(IllegalStateException.class);
            assertThat(request.getFailedWithUnknownReason().getMessage())
                    .contains("Transaction was not started");
            // do not recycle if a fail-over is triggered;
            // else there may be a race-condition in creating a new transaction with the same name
            assertThat(backchannel).doesNotHave(transactionFinished(false));
        }
    }

    static Stream<RuntimeException> rejectedCommits() {
        return Stream.of(
                new ProducerFencedException("test"),
                new InvalidTxnStateException("test"),
                new UnknownProducerIdException("test"));
    }

    /**
     * A committable with the producer of the writer is a transaction that this task has written in
     * this attempt. If the broker rejects its commit, the records are missing from the output.
     */
    @ParameterizedTest
    @MethodSource("rejectedCommits")
    public void testFailJobOnRejectedCommitOfWriterTransaction(RuntimeException rejection)
            throws IOException, InterruptedException {
        Properties properties = getProperties();
        properties.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "1234");
        try (final KafkaCommitter committer =
                        new KafkaCommitter(
                                properties, TRANS_ID, SUB_ID, ATTEMPT, false, MOCK_FACTORY);
                FlinkKafkaInternalProducer<?, ?> producer =
                        new MockProducer(properties, rejection);
                ReadableBackchannel<TransactionFinished> backchannel =
                        BackchannelFactory.getInstance()
                                .getReadableBackchannel(SUB_ID, ATTEMPT, TRANS_ID)) {
            final RecordingCommitRequest request =
                    new RecordingCommitRequest(KafkaCommittable.of(producer));
            committer.commit(Collections.singletonList(request));

            assertThat(request.getFailedWithUnknownReason())
                    .as("the task fails")
                    .isInstanceOf(FlinkRuntimeException.class)
                    .hasMessageContaining("'" + TRANS_ID + "'")
                    .hasMessageContaining(rejection.getClass().getSimpleName())
                    .hasMessageContaining("can no longer commit the transaction")
                    .hasMessageContaining("lost")
                    .hasCause(rejection);
            if (!(rejection instanceof UnknownProducerIdException)) {
                assertThat(request.getFailedWithUnknownReason())
                        .as("the message names both ways to lose a transaction")
                        .hasMessageContaining("'transactional.id'")
                        .hasMessageContaining("'transaction.timeout.ms' (1234ms)");
            }
            assertThat(request.failedWithKnownReason).isNull();
            // do not recycle if a fail-over is triggered;
            // else there may be a race-condition in creating a new transaction with the same name
            assertThat(backchannel).doesNotHave(transactionFinished(false));
        }
    }

    /**
     * A committable without a producer was restored from state, or its writer is not chained to the
     * committer. A rejected commit is expected in some cases then, and it is logged and dropped.
     */
    @ParameterizedTest
    @MethodSource("rejectedCommits")
    public void testDropRejectedCommitOfRestoredTransaction(RuntimeException rejection)
            throws IOException, InterruptedException {
        Properties properties = getProperties();
        try (final KafkaCommitter committer =
                        new KafkaCommitter(
                                properties,
                                TRANS_ID,
                                SUB_ID,
                                ATTEMPT,
                                false,
                                (props, transactionalId) -> new MockProducer(props, rejection));
                ReadableBackchannel<TransactionFinished> backchannel =
                        BackchannelFactory.getInstance()
                                .getReadableBackchannel(SUB_ID, ATTEMPT, TRANS_ID)) {
            final RecordingCommitRequest request =
                    new RecordingCommitRequest(
                            new KafkaCommittable(PRODUCER_ID, EPOCH, TRANS_ID, null));
            committer.commit(Collections.singletonList(request));

            assertThat(request.getFailedWithUnknownReason()).as("the task goes on").isNull();
            assertThat(request.failedWithKnownReason).isSameAs(rejection);
            assertThat(errorLogger.getMessages())
                    .anyMatch(message -> message.contains("Unable to commit transaction"))
                    .as("the task does not fail, so the log must not announce a restart")
                    .noneMatch(message -> message.contains("restart"));
            assertThat(backchannel).has(transactionFinished(false));
            assertThat(committer.getCommittingProducer()).isNull();
        }
    }

    @Test
    public void testCommitterProducerClosedOnError() throws IOException, InterruptedException {
        Properties properties = getProperties();
        AtomicInteger creationCounter = new AtomicInteger();
        BiFunction<Properties, String, FlinkKafkaInternalProducer<?, ?>> failingFactory =
                (props, transactionalId) -> {
                    creationCounter.incrementAndGet();
                    return new MockProducer(props, new ProducerFencedException("test"));
                };
        try (final KafkaCommitter committer =
                        new KafkaCommitter(
                                properties, TRANS_ID, SUB_ID, ATTEMPT, false, failingFactory);
                ReadableBackchannel<TransactionFinished> backchannel =
                        BackchannelFactory.getInstance()
                                .getReadableBackchannel(SUB_ID, ATTEMPT, TRANS_ID)) {
            final MockCommitRequest<KafkaCommittable> request =
                    new MockCommitRequest<>(new KafkaCommittable(0, (short) 0, TRANS_ID, null));
            committer.commit(Collections.singletonList(request));

            // will not reuse the producer in case of error
            assertThat(backchannel).has(transactionFinished(false));
            assertThat(committer.getCommittingProducer()).isNull();
            assertThat(creationCounter.get()).isEqualTo(1);

            // create a second producer
            committer.commit(Collections.singletonList(request));

            assertThat(backchannel).has(transactionFinished(false));
            assertThat(committer.getCommittingProducer()).isNull();
            assertThat(creationCounter.get()).isEqualTo(2);
        }
    }

    @Test
    public void testInterrupt() throws IOException {
        ServerSocket serverSocket = new ServerSocket(0);
        Properties properties = getProperties();
        properties.put(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                "http://localhost:" + serverSocket.getLocalPort());
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "1000");
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, String.valueOf(Long.MAX_VALUE));
        try (final KafkaCommitter committer =
                        new KafkaCommitter(
                                properties, TRANS_ID, SUB_ID, ATTEMPT, false, MOCK_FACTORY);
                FlinkKafkaInternalProducer<Object, Object> producer =
                        new FlinkKafkaInternalProducer<>(properties, TRANS_ID);
                ReadableBackchannel<TransactionFinished> backchannel =
                        BackchannelFactory.getInstance()
                                .getReadableBackchannel(SUB_ID, ATTEMPT, TRANS_ID)) {
            final MockCommitRequest<KafkaCommittable> request =
                    new MockCommitRequest<>(KafkaCommittable.of(producer));

            producer.resumeTransaction(PRODUCER_ID, EPOCH);

            AtomicBoolean interrupting = interruptOnMessage(Thread.currentThread(), serverSocket);
            assertThatThrownBy(() -> committer.commit(Collections.singletonList(request)))
                    .isInstanceOf(InterruptedException.class);

            // verify that the interrupt happened only after committing started
            assertThat(interrupting).isTrue();

            // no errors are logged
            assertThat(errorLogger.getMessages()).isEmpty();

            assertThat(backchannel).doesNotHave(transactionFinished(true));
        }
    }

    private AtomicBoolean interruptOnMessage(Thread mainThread, ServerSocket serverSocket) {
        final AtomicBoolean interrupting = new AtomicBoolean();
        new Thread(
                        () -> {
                            try {
                                serverSocket.accept().getInputStream().read();
                                interrupting.set(true);
                                mainThread.interrupt();
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        },
                        "canceller")
                .start();
        return interrupting;
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testKafkaCommitterRecyclesTransactionalId(boolean hasProducer)
            throws IOException, InterruptedException {
        Properties properties = getProperties();
        try (FlinkKafkaInternalProducer<?, ?> producer = new MockProducer(properties, null);
                final KafkaCommitter committer =
                        new KafkaCommitter(
                                properties, TRANS_ID, SUB_ID, ATTEMPT, false, MOCK_FACTORY);
                ReadableBackchannel<TransactionFinished> backchannel =
                        BackchannelFactory.getInstance()
                                .getReadableBackchannel(SUB_ID, ATTEMPT, TRANS_ID)) {
            final MockCommitRequest<KafkaCommittable> request =
                    new MockCommitRequest<>(
                            new KafkaCommittable(
                                    PRODUCER_ID, EPOCH, TRANS_ID, hasProducer ? producer : null));

            committer.commit(Collections.singletonList(request));
            assertThat(backchannel).has(transactionFinished(true));
            // will reuse the committer producer if no producer is passed
            assertThat(committer.getCommittingProducer() == null).isEqualTo(hasProducer);
        }
    }

    private Condition<? super ReadableBackchannel<TransactionFinished>> transactionFinished(
            boolean success) {
        return new Condition<>(
                backchannel -> {
                    TransactionFinished polled = backchannel.poll();
                    return polled != null && polled.isSuccess() == success;
                },
                "recycled producer");
    }

    Properties getProperties() {
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:1");
        // Low timeout will fail commitTransaction quicker
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "100");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return properties;
    }

    /** Also records the failures that do not fail the task. */
    private static class RecordingCommitRequest extends MockCommitRequest<KafkaCommittable> {
        private Throwable failedWithKnownReason;

        private RecordingCommitRequest(KafkaCommittable committable) {
            super(committable);
        }

        @Override
        public void signalFailedWithKnownReason(Throwable t) {
            failedWithKnownReason = t;
        }
    }

    private static class MockProducer extends FlinkKafkaInternalProducer<byte[], byte[]> {
        private final RuntimeException commitException;

        public MockProducer(Properties properties, RuntimeException commitException) {
            super(properties, KafkaCommitterTest.TRANS_ID);
            this.commitException = commitException;
        }

        @Override
        public void commitTransaction() throws ProducerFencedException {
            if (commitException != null) {
                throw commitException;
            }
        }

        @Override
        public void flush() {}
    }
}
