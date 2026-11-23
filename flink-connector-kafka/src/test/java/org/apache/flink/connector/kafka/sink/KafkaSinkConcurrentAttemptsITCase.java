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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource.NumberSequenceSplit;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiter;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.HeartbeatManagerOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.kafka.lineage.KafkaDatasetFacet;
import org.apache.flink.connector.kafka.lineage.KafkaDatasetFacetProvider;
import org.apache.flink.connector.kafka.testutils.KafkaUtil;
import org.apache.flink.connector.kafka.testutils.TestKafkaContainer;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.webmonitor.ClusterOverview;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.testutils.junit.SharedObjectsExtension;
import org.apache.flink.testutils.junit.SharedReference;
import org.apache.flink.util.IOUtils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs an exactly-once sink pipeline while two execution attempts of the sink subtask are alive at
 * the same time.
 *
 * <p>The JobManager fails the execution attempts of a TaskManager that stops answering its
 * heartbeats and deploys the next attempts without a confirmation from that TaskManager. The tasks
 * of the earlier attempts go on until their TaskManager notices. The test produces this situation
 * with a slow attempt, which waits in the constructor of its sink writer, and by blocking the main
 * thread of the TaskManager that runs it: that thread serializes the user accumulators of the tasks
 * for the heartbeat to the JobManager.
 *
 * <p>The slow attempt is released when a later attempt has flushed and pre-committed records for a
 * checkpoint that is not complete yet. The writer that the slow attempt initializes now aborts the
 * transactions of earlier executions and opens its own, and it has to leave the transaction of the
 * later attempt alone. The test covers attempts that start without a checkpoint and attempts that
 * restore one.
 */
@Testcontainers
class KafkaSinkConcurrentAttemptsITCase {
    private static final Duration TIMEOUT = Duration.ofMinutes(2);
    private static final int RECORD_COUNT = 50;
    private static final int RECORDS_PER_CHECKPOINT = 10;

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(2)
                            .setNumberSlotsPerTaskManager(1)
                            .setConfiguration(clusterConfiguration())
                            .build());

    @Container
    static final TestKafkaContainer KAFKA =
            KafkaUtil.createKafkaContainer(KafkaSinkConcurrentAttemptsITCase.class);

    @RegisterExtension final SharedObjectsExtension sharedObjects = SharedObjectsExtension.create();

    private final List<AutoCloseable> cleanupActions = new ArrayList<>();

    @AfterEach
    void tearDown() throws Throwable {
        cleanupActions.add(KafkaUtil::checkProducerLeak);
        IOUtils.closeAll(cleanupActions, Throwable.class);
    }

    private static Configuration clusterConfiguration() {
        final Configuration configuration = new Configuration();
        // the test waits this long until the JobManager gives up on the silent TaskManager
        configuration.set(HeartbeatManagerOptions.HEARTBEAT_INTERVAL, Duration.ofMillis(500));
        configuration.set(HeartbeatManagerOptions.HEARTBEAT_TIMEOUT, Duration.ofSeconds(5));
        return configuration;
    }

    static Stream<Arguments> namingStrategiesAndStartingPoints() {
        return Arrays.stream(TransactionNamingStrategy.values())
                .flatMap(strategy -> Stream.of(false, true).map(r -> Arguments.of(strategy, r)));
    }

    /**
     * Tests that every record of the source is committed exactly once when an earlier attempt of
     * the sink subtask initializes its writer while a later attempt waits for the completion of a
     * checkpoint. The earlier writer may start or refuse to start; the test accepts both.
     */
    @ParameterizedTest(name = "{0}, attemptsRestoreCheckpoint={1}")
    @MethodSource("namingStrategiesAndStartingPoints")
    void shouldCommitAllRecordsWhenEarlierAttemptCreatesWriterLate(
            TransactionNamingStrategy namingStrategy,
            boolean attemptsRestoreCheckpoint,
            @InjectMiniCluster MiniCluster miniCluster,
            @InjectClusterClient ClusterClient<?> clusterClient)
            throws Exception {
        // the later attempt needs a slot on a TaskManager that still answers the JobManager
        waitForTwoIdleTaskManagers(miniCluster);

        final String topic = "concurrent-attempts-" + UUID.randomUUID();
        final Properties kafkaProperties = kafkaProperties();
        KafkaUtil.createNewTopicAndWaitForPartitionAssignment(topic, 1, 1, kafkaProperties);
        cleanupActions.add(
                () -> {
                    try (AdminClient admin = AdminClient.create(kafkaProperties)) {
                        admin.deleteTopics(Collections.singleton(topic))
                                .all()
                                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                    }
                });

        // With a restored checkpoint, attempt 0 writes a batch and fails after a checkpoint that
        // leaves no transaction pending, see AttemptControl. Attempt 1 is then the slow attempt,
        // and it restores the same checkpoint as the attempts after it.
        final SharedReference<PipelineControls> controls =
                sharedObjects.add(new PipelineControls(attemptsRestoreCheckpoint ? 1 : 0));
        cleanupActions.add(0, controls.get()::releaseAll);

        final JobID jobId =
                clusterClient
                        .submitJob(
                                createJob(topic, namingStrategy, controls)
                                        .getStreamGraph()
                                        .getJobGraph())
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        cleanupActions.add(0, () -> cancelIfRunning(clusterClient, jobId));

        await(
                controls.get().slowAttemptAtWriterOrEnded,
                "the slow attempt reached the constructor of its sink writer");
        assertThat(controls.get().slowAttemptReachedWriter)
                .as(
                        "Precondition not met: the slow attempt ended before it reached the "
                                + "constructor of its sink writer, so this run cannot create "
                                + "the writer of an earlier attempt late")
                .isTrue();
        controls.get().stallTaskManagerOfSlowAttempt();
        await(
                controls.get().taskManagerStalled,
                "the TaskManager of the slow attempt stopped answering heartbeats");

        // only reached after the JobManager gave up on the slow attempt and deployed another one
        await(
                controls.get().laterAttemptPrecommitted,
                "a later attempt pre-committed records for a checkpoint");
        controls.get().releaseSlowAttempt.countDown();
        // the later attempt keeps its checkpoint open until the slow attempt is past this point
        await(
                controls.get().slowAttemptPastWriterInitialization,
                "the slow attempt initialized its sink writer or failed to do so");
        assertThat(controls.get().slowAttemptStartedWriterInitialization)
                .as(
                        "Precondition not met: the slow attempt ended in the constructor of "
                                + "its sink writer, before the writer was initialized")
                .isTrue();

        final JobResult jobResult =
                clusterClient
                        .requestJobResult(jobId)
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assertThat(jobResult.isSuccess())
                .as("job finished successfully: %s", jobResult.getSerializedThrowable())
                .isTrue();
        await(
                controls.get().precommittedCheckpointCompleted,
                "the checkpoint with the pre-committed records completed");

        // the slow attempt holds its writer until here; it must be gone before the final read
        controls.get().endSlowAttempt.countDown();
        await(controls.get().slowAttemptEnded, "the slow attempt ended");

        // the broker ends a transaction in the partition after it has answered the producer
        KafkaUtil.waitUntilNoTransactionIsOpen(topic, kafkaProperties);
        final List<Long> committedRecords =
                KafkaUtil.drainAllRecordsFromTopic(topic, kafkaProperties, true).stream()
                        .map(KafkaSinkConcurrentAttemptsITCase::decodeRecord)
                        .collect(Collectors.toList());
        assertThat(controls.get().emittedRecords)
                .as("records emitted by the source")
                .hasSize(RECORD_COUNT);
        assertThat(committedRecords)
                .as(
                        "committed records; a missing record means that the writer of the "
                                + "earlier attempt ended the transaction that a later attempt "
                                + "had pre-committed for a checkpoint")
                .containsExactlyInAnyOrderElementsOf(controls.get().emittedRecords);
    }

    private StreamExecutionEnvironment createJob(
            String topic,
            TransactionNamingStrategy namingStrategy,
            SharedReference<PipelineControls> controls) {
        final Configuration configuration = new Configuration();
        configuration.set(CoreOptions.DEFAULT_PARALLELISM, 1);
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 10);
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofMillis(100));
        final StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        environment.enableCheckpointing(100L);

        final DataGeneratorSource<Long> source =
                new DataGeneratorSource<>(
                        value -> value,
                        RECORD_COUNT,
                        parallelism -> new BatchRateLimiter(controls),
                        BasicTypeInfo.LONG_TYPE_INFO);
        // source, attempt control and sink writer have to be chained, see AttemptControl
        environment
                .fromSource(source, WatermarkStrategy.noWatermarks(), "source")
                .map(new AttemptControl(controls))
                .sinkTo(
                        KafkaSink.<Long>builder()
                                .setBootstrapServers(KAFKA.getBootstrapServers())
                                .setTransactionalIdPrefix(
                                        "concurrent-attempts-" + UUID.randomUUID())
                                .setTransactionNamingStrategy(namingStrategy)
                                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                                .setRecordSerializer(new WriterConstructorGate(topic, controls))
                                .build());
        return environment;
    }

    private static void waitForTwoIdleTaskManagers(MiniCluster miniCluster) throws Exception {
        CommonTestUtils.waitUtil(
                () -> {
                    try {
                        final ClusterOverview overview =
                                miniCluster
                                        .requestClusterOverview()
                                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                        return overview.getNumTaskManagersConnected() == 2
                                && overview.getNumSlotsAvailable() == 2;
                    } catch (Exception e) {
                        return false;
                    }
                },
                TIMEOUT,
                Duration.ofMillis(100),
                "The cluster does not have two TaskManagers with a free slot each");
    }

    private static void await(CountDownLatch latch, String description)
            throws InterruptedException {
        assertThat(latch.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .as("Timed out waiting until %s", description)
                .isTrue();
    }

    private static void cancelIfRunning(ClusterClient<?> clusterClient, JobID jobId)
            throws Exception {
        if (!clusterClient
                .getJobStatus(jobId)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                .isGloballyTerminalState()) {
            clusterClient.cancel(jobId).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            clusterClient.requestJobResult(jobId).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    private static Properties kafkaProperties() {
        final Properties properties = new Properties();
        properties.put("bootstrap.servers", KAFKA.getBootstrapServers());
        properties.put("group.id", UUID.randomUUID().toString());
        properties.put("enable.auto.commit", false);
        properties.put("auto.offset.reset", "earliest");
        return properties;
    }

    private static long decodeRecord(ConsumerRecord<byte[], byte[]> record) {
        return ByteBuffer.wrap(record.value()).getLong();
    }

    private static final class RecordSerializer implements SerializationSchema<Long> {
        @Override
        public byte[] serialize(Long value) {
            return ByteBuffer.allocate(Long.BYTES).putLong(value).array();
        }
    }

    /**
     * Coordinates the test thread with the attempts of the task and with one TaskManager. An
     * attempt is told apart by its task thread, which builds the operator chain, initializes the
     * operators and runs the source, the map function and the sink writer.
     */
    private static final class PipelineControls {
        private static final long NO_CHECKPOINT = -1L;

        private final int slowAttempt;
        private final Set<Long> emittedRecords = new ConcurrentSkipListSet<>();

        private volatile Thread slowAttemptThread;
        private volatile boolean slowAttemptReachedWriter;
        private volatile boolean slowAttemptStartedWriterInitialization;
        private final CountDownLatch slowAttemptAtWriterOrEnded = new CountDownLatch(1);
        private final CountDownLatch releaseSlowAttempt = new CountDownLatch(1);
        private final CountDownLatch slowAttemptPastWriterInitialization = new CountDownLatch(1);
        private final CountDownLatch endSlowAttempt = new CountDownLatch(1);
        private final CountDownLatch slowAttemptEnded = new CountDownLatch(1);

        private volatile boolean stallTaskManager;
        private final CountDownLatch taskManagerStalled = new CountDownLatch(1);
        private final CountDownLatch releaseTaskManager = new CountDownLatch(1);

        private volatile Thread taskWithWaitingSource;
        private volatile CompletableFuture<Void> nextBatch;
        private volatile long checkpointCoveringBatch = NO_CHECKPOINT;

        private final AtomicBoolean checkpointHeld = new AtomicBoolean();
        private volatile long precommittedCheckpointId = NO_CHECKPOINT;
        private final CountDownLatch laterAttemptPrecommitted = new CountDownLatch(1);
        private final CountDownLatch precommittedCheckpointCompleted = new CountDownLatch(1);

        private PipelineControls(int slowAttempt) {
            this.slowAttempt = slowAttempt;
        }

        /** Called in the constructor of a sink writer, before the writer talks to the broker. */
        private void holdSlowAttemptInWriterConstructor() throws InterruptedException {
            if (Thread.currentThread() != slowAttemptThread) {
                return;
            }
            slowAttemptReachedWriter = true;
            slowAttemptAtWriterOrEnded.countDown();
            // an interrupt means that the attempt was cancelled and will not initialize its writer
            releaseSlowAttempt.await();
            slowAttemptStartedWriterInitialization = true;
        }

        private void stallTaskManagerOfSlowAttempt() {
            stallTaskManager = true;
        }

        /** Runs in the main thread of a TaskManager, which is not meant to be interrupted. */
        private void stallTaskManagerIfRequested(int attempt) {
            if (attempt != slowAttempt || !stallTaskManager) {
                return;
            }
            taskManagerStalled.countDown();
            boolean interrupted = false;
            while (releaseTaskManager.getCount() > 0) {
                try {
                    releaseTaskManager.await();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }

        /** The source of the calling task has emitted a batch and waits for the next one. */
        private CompletableFuture<Void> sourceWaitsForNextBatch() {
            final CompletableFuture<Void> waiting = new CompletableFuture<>();
            checkpointCoveringBatch = NO_CHECKPOINT;
            nextBatch = waiting;
            taskWithWaitingSource = Thread.currentThread();
            return waiting;
        }

        private boolean sourceOfThisTaskWaits() {
            return taskWithWaitingSource == Thread.currentThread();
        }

        /** Whether the checkpoint was taken after the waiting source had emitted its batch. */
        private boolean coversBatchOfThisTask(long checkpointId) {
            return sourceOfThisTaskWaits()
                    && checkpointCoveringBatch != NO_CHECKPOINT
                    && checkpointId >= checkpointCoveringBatch;
        }

        private void startNextBatchOfThisTask() {
            final CompletableFuture<Void> waiting = nextBatch;
            taskWithWaitingSource = null;
            waiting.complete(null);
        }

        private void checkpointCompletedOrRestored(long checkpointId) {
            if (checkpointId == precommittedCheckpointId) {
                precommittedCheckpointCompleted.countDown();
            }
        }

        private void releaseAll() {
            releaseSlowAttempt.countDown();
            slowAttemptPastWriterInitialization.countDown();
            endSlowAttempt.countDown();
            releaseTaskManager.countDown();
        }
    }

    /**
     * Gives every attempt of the task its role. It is chained between the source and the sink
     * writer and relies on the order in which a task works on its chain: the runtime context is set
     * when the chain is built, before any operator is initialized; operators are initialized and
     * opened from the end of the chain; on a checkpoint all operators prepare for the barrier,
     * which is where the sink writer flushes and pre-commits, before the first one takes its
     * snapshot.
     */
    private static final class AttemptControl extends RichMapFunction<Long, Long>
            implements CheckpointedFunction, CheckpointListener {
        private final SharedReference<PipelineControls> controls;
        private transient int attempt;
        private transient int recordsSinceLastSnapshot;
        private transient long lastCheckpointWithRecords;
        private transient long lastCompletedCheckpoint;
        private transient long checkpointWithoutPendingTransactions;

        private AttemptControl(SharedReference<PipelineControls> controls) {
            this.controls = controls;
        }

        private boolean isSlowAttempt() {
            return attempt == controls.get().slowAttempt;
        }

        private boolean isLaterAttempt() {
            return attempt > controls.get().slowAttempt;
        }

        private boolean isEarlierAttempt() {
            return attempt < controls.get().slowAttempt;
        }

        @Override
        public void setRuntimeContext(RuntimeContext context) {
            super.setRuntimeContext(context);
            attempt = context.getTaskInfo().getAttemptNumber();
            lastCheckpointWithRecords = PipelineControls.NO_CHECKPOINT;
            lastCompletedCheckpoint = PipelineControls.NO_CHECKPOINT;
            checkpointWithoutPendingTransactions = PipelineControls.NO_CHECKPOINT;
            context.addAccumulator(
                    "task-manager-stall", new TaskManagerStallingAccumulator(controls, attempt));
            if (isSlowAttempt()) {
                controls.get().slowAttemptThread = Thread.currentThread();
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) {
            if (isLaterAttempt()) {
                context.getRestoredCheckpointId()
                        .ifPresent(controls.get()::checkpointCompletedOrRestored);
            }
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            if (isSlowAttempt()) {
                // the sink writer of this attempt is initialized, the source has not started
                controls.get().slowAttemptPastWriterInitialization.countDown();
                controls.get().endSlowAttempt.await();
                throw new IllegalStateException("The slow attempt ends without writing a record");
            }
        }

        @Override
        public void close() {
            if (isSlowAttempt()) {
                // The attempt ends: before it reached its sink writer, in the initialization of
                // the writer, or at the end of the test. The test tells these apart.
                controls.get().slowAttemptAtWriterOrEnded.countDown();
                controls.get().slowAttemptPastWriterInitialization.countDown();
                controls.get().slowAttemptEnded.countDown();
            }
        }

        @Override
        public Long map(Long value) {
            controls.get().emittedRecords.add(value);
            recordsSinceLastSnapshot++;
            return value;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            final PipelineControls pipeline = controls.get();
            final long checkpointId = context.getCheckpointId();
            if (pipeline.sourceOfThisTaskWaits()) {
                if (pipeline.checkpointCoveringBatch == PipelineControls.NO_CHECKPOINT) {
                    pipeline.checkpointCoveringBatch = checkpointId;
                }
                // No record follows before this checkpoint is complete. Without that, the rest
                // of a batch could be written to a transaction that the slow attempt has ended,
                // and the task could fail before the checkpoint completes.
                if (isLaterAttempt()
                        && recordsSinceLastSnapshot > 0
                        && pipeline.checkpointHeld.compareAndSet(false, true)) {
                    // the sink writer has flushed and pre-committed the records of the checkpoint
                    pipeline.precommittedCheckpointId = checkpointId;
                    pipeline.laterAttemptPrecommitted.countDown();
                    // the checkpoint cannot complete before this method returns
                    pipeline.slowAttemptPastWriterInitialization.await();
                }
                // The transactions with the records of this attempt are committed, the committer
                // has told the writer, and no record was written since. So the writer state of
                // this checkpoint lists no pre-committed transaction, see
                // #notifyCheckpointComplete.
                if (isEarlierAttempt()
                        && recordsSinceLastSnapshot == 0
                        && lastCheckpointWithRecords != PipelineControls.NO_CHECKPOINT
                        && lastCompletedCheckpoint >= lastCheckpointWithRecords
                        && checkpointWithoutPendingTransactions == PipelineControls.NO_CHECKPOINT) {
                    checkpointWithoutPendingTransactions = checkpointId;
                }
            }
            if (recordsSinceLastSnapshot > 0) {
                lastCheckpointWithRecords = checkpointId;
            }
            recordsSinceLastSnapshot = 0;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            final PipelineControls pipeline = controls.get();
            lastCompletedCheckpoint = Math.max(lastCompletedCheckpoint, checkpointId);
            if (isLaterAttempt()) {
                pipeline.checkpointCompletedOrRestored(checkpointId);
                if (pipeline.coversBatchOfThisTask(checkpointId)) {
                    pipeline.startNextBatchOfThisTask();
                }
            } else if (isEarlierAttempt()
                    && checkpointWithoutPendingTransactions != PipelineControls.NO_CHECKPOINT
                    && checkpointId >= checkpointWithoutPendingTransactions) {
                // An attempt before the slow one writes one batch and fails here, so that the
                // following attempts restore a checkpoint. The writer state of that checkpoint
                // lists no pre-committed transaction. The cleanup of a restored writer spares
                // the transactions of that list, and the test must not pass because the slow
                // attempt spares a transaction for that reason.
                throw new IllegalStateException(
                        "Planned failure after a checkpoint without pending transactions");
            }
        }
    }

    /**
     * Lets the source emit a batch of records and then wait until the {@link AttemptControl} of the
     * same task starts the next batch. A later attempt does so when a checkpoint is complete that
     * was taken after the whole batch; compared to {@code RateLimiterStrategy#perCheckpoint}, the
     * completion of a checkpoint whose barrier fell into the batch does not count.
     */
    private static final class BatchRateLimiter implements RateLimiter<NumberSequenceSplit> {
        private final SharedReference<PipelineControls> controls;
        private int permitsLeft = RECORDS_PER_CHECKPOINT;

        private BatchRateLimiter(SharedReference<PipelineControls> controls) {
            this.controls = controls;
        }

        @Override
        public CompletionStage<Void> acquire(int numberOfEvents) {
            if (permitsLeft > 0) {
                permitsLeft--;
                return CompletableFuture.completedFuture(null);
            }
            // the request that waits takes the first permit of the next batch
            permitsLeft = RECORDS_PER_CHECKPOINT - 1;
            return controls.get().sourceWaitsForNextBatch();
        }
    }

    /**
     * The constructor of the sink writer opens the record serializer before the writer aborts
     * transactions of earlier executions and starts its own. The slow attempt waits here.
     */
    private static final class WriterConstructorGate
            implements KafkaRecordSerializationSchema<Long>, KafkaDatasetFacetProvider {
        private final KafkaRecordSerializationSchema<Long> serializer;
        private final SharedReference<PipelineControls> controls;

        private WriterConstructorGate(String topic, SharedReference<PipelineControls> controls) {
            this.serializer =
                    KafkaRecordSerializationSchema.builder()
                            .setTopic(topic)
                            .setValueSerializationSchema(new RecordSerializer())
                            .build();
            this.controls = controls;
        }

        @Override
        public void open(
                SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext)
                throws Exception {
            serializer.open(context, sinkContext);
            controls.get().holdSlowAttemptInWriterConstructor();
        }

        @Nullable
        @Override
        public ProducerRecord<byte[], byte[]> serialize(
                Long element, KafkaSinkContext context, Long timestamp) {
            return serializer.serialize(element, context, timestamp);
        }

        @Override
        public Optional<KafkaDatasetFacet> getKafkaDatasetFacet() {
            return ((KafkaDatasetFacetProvider) serializer).getKafkaDatasetFacet();
        }
    }

    /**
     * A TaskManager serializes the user accumulators of its tasks in its main thread to report them
     * with the heartbeat to the JobManager. While this accumulator blocks there, the TaskManager
     * answers no heartbeat and handles no call of the JobManager, and its tasks go on.
     */
    private static final class TaskManagerStallingAccumulator extends IntCounter {
        private final SharedReference<PipelineControls> controls;
        private final int attempt;

        private TaskManagerStallingAccumulator(
                SharedReference<PipelineControls> controls, int attempt) {
            this.controls = controls;
            this.attempt = attempt;
        }

        private void writeObject(ObjectOutputStream out) throws IOException {
            controls.get().stallTaskManagerIfRequested(attempt);
            out.defaultWriteObject();
        }
    }
}
