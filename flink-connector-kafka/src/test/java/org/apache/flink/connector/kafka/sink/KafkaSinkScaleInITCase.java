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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource.NumberSequenceSplit;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.kafka.lineage.DefaultKafkaDatasetFacet;
import org.apache.flink.connector.kafka.lineage.DefaultKafkaDatasetIdentifier;
import org.apache.flink.connector.kafka.lineage.KafkaDatasetFacet;
import org.apache.flink.connector.kafka.lineage.KafkaDatasetFacetProvider;
import org.apache.flink.connector.kafka.testutils.KafkaUtil;
import org.apache.flink.connector.kafka.testutils.TestKafkaContainer;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.messages.webmonitor.ClusterOverview;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.testutils.junit.SharedObjectsExtension;
import org.apache.flink.testutils.junit.SharedReference;
import org.apache.flink.util.IOUtils;

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.connector.kafka.sink.internal.TransactionalIdFactory.buildTransactionalId;
import static org.apache.flink.connector.kafka.sink.internal.TransactionalIdFactory.extractSubtaskId;
import static org.apache.flink.connector.kafka.testutils.KafkaUtil.getOpenTransactionalIds;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises the cleanup of transactions with a real sink pipeline when an attempt with fewer
 * subtasks follows an attempt that ended before its first checkpoint and had lost a writer.
 *
 * <p>The job runs with the adaptive scheduler on six task managers with one slot each. In the first
 * attempt, the subtask with the highest number writes a record and then stops responding, even to
 * its cancellation. One other subtask fails before it has created its writer, and it does so only
 * after every remaining subtask has written a record. That failure ends the attempt. The task
 * manager of the unresponsive subtask is shut down once {@code task.cancellation.timeout} has
 * passed, so its writer is never closed and its transaction stays open. The scheduler then restarts
 * the job with the five slots that are left. Of these five subtasks, subtask 0 is responsible for
 * the transactions of the lost subtask 5.
 *
 * <p>Once all writers of the second attempt have started, the transaction of the lost subtask must
 * no longer be open: it would keep {@code read_committed} consumers from reading what the job
 * commits to the same partition later.
 *
 * <p>The record serializer exposes its topic. The tests therefore do not prescribe how a cleanup
 * finds the transaction: probing transactional ids and looking at the target topics both work.
 */
@Testcontainers
class KafkaSinkScaleInITCase {
    private static final Duration TIMEOUT = Duration.ofMinutes(2);
    private static final int PARALLELISM = 6;
    private static final int LOST_SUBTASK = PARALLELISM - 1;

    /** Marks an assertion that is expected to fail, so that a red build is not taken for new. */
    private static final String KNOWN_DEFECT =
            "KNOWN, OPEN DEFECT - this failure is expected until it is fixed: with INCREMENTING, "
                    + "a writer does not find a transaction behind a gap in the subtask numbers "
                    + "of an earlier attempt. ";

    private static final String OPEN_TRANSACTIONS_OF_LOST_SUBTASK =
            "Open transactions of the lost subtask after all writers of the second attempt have "
                    + "started";

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(PARALLELISM)
                            .setNumberSlotsPerTaskManager(1)
                            .setConfiguration(clusterConfiguration())
                            .build());

    @Container
    static final TestKafkaContainer KAFKA =
            KafkaUtil.createKafkaContainer(KafkaSinkScaleInITCase.class);

    @RegisterExtension final SharedObjectsExtension sharedObjects = SharedObjectsExtension.create();

    private final List<AutoCloseable> cleanupActions = new ArrayList<>();

    @AfterEach
    void tearDown() throws Throwable {
        cleanupActions.add(KafkaUtil::checkProducerLeak);
        IOUtils.closeAll(cleanupActions, Throwable.class);
    }

    /**
     * The subtask that fails before it has created its writer is subtask 1. Subtask 0 of the first
     * attempt had started, so subtask 0 of the second attempt goes on to the lost subtask 5.
     */
    @Test
    void abortsTransactionOfLostSubtaskAfterScaleIn(@InjectMiniCluster MiniCluster miniCluster)
            throws Exception {
        assertThat(getOpenTransactionsOfLostSubtaskAfterScaleIn(1, miniCluster))
                .as(OPEN_TRANSACTIONS_OF_LOST_SUBTASK)
                .isEmpty();
    }

    /**
     * The subtask that fails before it has created its writer is subtask 0, which leaves a gap in
     * front of the lost subtask 5 in the subtask numbers that the first attempt had used.
     *
     * <p>This test fails until the cleanup of {@link TransactionNamingStrategy#INCREMENTING} also
     * finds a transaction behind such a gap.
     */
    @Test
    void abortsTransactionOfLostSubtaskBehindGapAfterScaleIn(
            @InjectMiniCluster MiniCluster miniCluster) throws Exception {
        assertThat(getOpenTransactionsOfLostSubtaskAfterScaleIn(0, miniCluster))
                .as(KNOWN_DEFECT + OPEN_TRANSACTIONS_OF_LOST_SUBTASK)
                .isEmpty();
    }

    /**
     * Runs the job through both attempts.
     *
     * @param subtaskFailingBeforeStart the subtask of the first attempt that fails before it has
     *     created its writer
     * @return the transactional ids of the lost subtask whose transactions are open once all
     *     writers of the second attempt have started
     */
    private List<String> getOpenTransactionsOfLostSubtaskAfterScaleIn(
            int subtaskFailingBeforeStart, MiniCluster miniCluster) throws Exception {
        final String topic = "scale-in-" + UUID.randomUUID();
        final String transactionalIdPrefix = "scale-in-" + UUID.randomUUID();
        final String lostTransactionalId =
                buildTransactionalId(transactionalIdPrefix, LOST_SUBTASK, 1);
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
        provideOneSlotForEachSubtask(miniCluster);

        final SharedReference<PipelineControls> controls =
                sharedObjects.add(new PipelineControls(subtaskFailingBeforeStart));
        startJob(topic, transactionalIdPrefix, controls);

        try (AdminClient admin = AdminClient.create(kafkaProperties)) {
            // First attempt: every subtask but the failing one has created its writer, which
            // registers the first transactional id of that subtask at the broker. Without this
            // wait, the attempt could end with more gaps than the test intends.
            await(
                    controls.get().startedWritersOfFirstAttempt,
                    "all writers of the first attempt but the failing one started");
            // The lost subtask has an open transaction and no longer responds.
            await(controls.get().lostSubtaskUnresponsive, "the lost subtask stopped responding");
            CommonTestUtils.waitUtil(
                    () -> getOpenTransactionalIds(admin, topic).contains(lostTransactionalId),
                    TIMEOUT,
                    Duration.ofMillis(100),
                    "The lost subtask did not open its transaction.");

            // The failure ends the attempt before the failing subtask has created its writer.
            controls.get().failSubtaskBeforeStart.countDown();

            // Second attempt: the scheduler has only the slots of the remaining task managers.
            await(
                    controls.get().startedWritersOfSecondAttempt,
                    "all writers of the second attempt started");
            return getOpenTransactionalIds(admin, topic).stream()
                    .filter(transactionalId -> transactionalId.startsWith(transactionalIdPrefix))
                    .filter(transactionalId -> extractSubtaskId(transactionalId) == LOST_SUBTASK)
                    .collect(Collectors.toList());
        }
    }

    private void startJob(
            String topic, String transactionalIdPrefix, SharedReference<PipelineControls> controls)
            throws Exception {
        final Configuration configuration = new Configuration();
        configuration.set(CoreOptions.DEFAULT_PARALLELISM, PARALLELISM);
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ZERO);
        final StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        // Exactly-once needs checkpointing. Long.MAX_VALUE disables periodic scheduling: the first
        // attempt ends before a checkpoint could complete, and the test looks at the second
        // attempt as soon as its writers have started.
        environment.enableCheckpointing(Long.MAX_VALUE);
        final DataGeneratorSource<Long> source =
                new DataGeneratorSource<>(
                        value -> value,
                        Long.MAX_VALUE,
                        parallelism -> new TwoRecordsRateLimiter(),
                        BasicTypeInfo.LONG_TYPE_INFO);
        environment
                .fromSource(source, WatermarkStrategy.noWatermarks(), "two records per subtask")
                .sinkTo(
                        KafkaSink.<Long>builder()
                                .setBootstrapServers(KAFKA.getBootstrapServers())
                                .setTransactionalIdPrefix(transactionalIdPrefix)
                                .setTransactionNamingStrategy(
                                        TransactionNamingStrategy.INCREMENTING)
                                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                                .setRecordSerializer(new ControlledSerializer(topic, controls))
                                .build());
        final JobClient job = environment.executeAsync("scale-in after a partly started attempt");
        cleanupActions.add(0, () -> cancelAndWait(job, controls.get()));
    }

    /**
     * Starts a task manager for each one that the cluster is missing, because an earlier test has
     * lost it, and waits until a free slot is registered for each subtask.
     */
    private static void provideOneSlotForEachSubtask(MiniCluster miniCluster) throws Exception {
        final int missingTaskManagers =
                PARALLELISM - getClusterOverview(miniCluster).getNumTaskManagersConnected();
        for (int i = 0; i < missingTaskManagers; i++) {
            miniCluster.startTaskManager();
        }
        CommonTestUtils.waitUtil(
                () -> getClusterOverview(miniCluster).getNumSlotsAvailable() == PARALLELISM,
                TIMEOUT,
                "The cluster does not have a free slot for each subtask.");
    }

    private static ClusterOverview getClusterOverview(MiniCluster miniCluster) {
        return miniCluster
                .requestClusterOverview()
                .orTimeout(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                .join();
    }

    private static void cancelAndWait(JobClient job, PipelineControls controls) throws Exception {
        try {
            try {
                job.cancel().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            } catch (ExecutionException cancellationFailure) {
                // Cancellation may be rejected for a terminal job. Ignore that rejection so
                // cleanup does not obscure an earlier test failure.
                if (!job.getJobStatus()
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                        .isGloballyTerminalState()) {
                    throw cancellationFailure;
                }
            }
            job.getJobExecutionResult()
                    .handle((result, failure) -> null)
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } finally {
            // Lets the task thread of the lost subtask end, which closes its producer.
            controls.releaseLostSubtask.countDown();
        }
    }

    private static void await(CountDownLatch latch, String description)
            throws InterruptedException {
        assertThat(latch.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as(description).isTrue();
    }

    private static Properties kafkaProperties() {
        final Properties properties = new Properties();
        properties.put("bootstrap.servers", KAFKA.getBootstrapServers());
        return properties;
    }

    private static Configuration clusterConfiguration() {
        final Configuration configuration = new Configuration();
        // After a failure, the adaptive scheduler restarts the job with the slots it has. Before
        // that it waits its default of 10 seconds for the slot of the lost task manager.
        configuration.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Adaptive);
        // Shuts down the task manager of a task that does not react to its cancellation. The
        // other tasks need far less than this time to close their writers.
        configuration.set(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT, Duration.ofSeconds(10));
        return configuration;
    }

    /** Coordinates the test thread and the subtasks of the two attempts. */
    private static final class PipelineControls {
        private final int subtaskFailingBeforeStart;
        private final CountDownLatch startedWritersOfFirstAttempt =
                new CountDownLatch(PARALLELISM - 1);
        private final CountDownLatch lostSubtaskUnresponsive = new CountDownLatch(1);
        private final CountDownLatch failSubtaskBeforeStart = new CountDownLatch(1);
        private final CountDownLatch startedWritersOfSecondAttempt =
                new CountDownLatch(PARALLELISM - 1);
        private final CountDownLatch releaseLostSubtask = new CountDownLatch(1);

        private PipelineControls(int subtaskFailingBeforeStart) {
            this.subtaskFailingBeforeStart = subtaskFailingBeforeStart;
        }
    }

    /** Lets each subtask emit two records and then holds it back for the rest of the attempt. */
    private static final class TwoRecordsRateLimiter implements RateLimiter<NumberSequenceSplit> {
        private int remainingRecords = 2;

        @Override
        public CompletionStage<Void> acquire(int numberOfEvents) {
            if (remainingRecords > 0) {
                remainingRecords--;
                return CompletableFuture.completedFuture(null);
            }
            return new CompletableFuture<>();
        }
    }

    /**
     * Serializer that stands in for what happens to the subtasks of the first attempt, which is the
     * attempt with {@link #PARALLELISM} subtasks, and reports the writers that have started in both
     * attempts.
     */
    private static final class ControlledSerializer
            implements KafkaRecordSerializationSchema<Long>, KafkaDatasetFacetProvider {
        private final String topic;
        private final SharedReference<PipelineControls> controls;
        private transient int serializedRecords;

        private ControlledSerializer(String topic, SharedReference<PipelineControls> controls) {
            this.topic = topic;
            this.controls = controls;
        }

        /** Runs when the writer is created, before the writer has talked to the broker. */
        @Override
        public void open(
                SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext)
                throws Exception {
            if (isFirstAttempt(sinkContext)
                    && sinkContext.getParallelInstanceId()
                            == controls.get().subtaskFailingBeforeStart) {
                controls.get().failSubtaskBeforeStart.await();
                throw new IOException("Subtask fails before it has created its writer.");
            }
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(
                Long element, KafkaSinkContext context, Long timestamp) {
            serializedRecords++;
            if (serializedRecords == 1) {
                // A writer serializes only after it has been created.
                if (isFirstAttempt(context)) {
                    controls.get().startedWritersOfFirstAttempt.countDown();
                } else {
                    controls.get().startedWritersOfSecondAttempt.countDown();
                }
            } else if (isFirstAttempt(context)
                    && context.getParallelInstanceId() == LOST_SUBTASK
                    && serializedRecords == 2) {
                // The first record is in the transaction. From here on, the task behaves like
                // one that hangs: it ignores the interrupts of the cancellation.
                controls.get().lostSubtaskUnresponsive.countDown();
                Uninterruptibles.awaitUninterruptibly(controls.get().releaseLostSubtask);
            }
            return new ProducerRecord<>(
                    topic, ByteBuffer.allocate(Long.BYTES).putLong(element).array());
        }

        @Override
        public Optional<KafkaDatasetFacet> getKafkaDatasetFacet() {
            return Optional.of(
                    new DefaultKafkaDatasetFacet(
                            DefaultKafkaDatasetIdentifier.ofTopics(
                                    Collections.singletonList(topic))));
        }

        private static boolean isFirstAttempt(KafkaSinkContext sinkContext) {
            return sinkContext.getNumberOfParallelInstances() == PARALLELISM;
        }
    }
}
