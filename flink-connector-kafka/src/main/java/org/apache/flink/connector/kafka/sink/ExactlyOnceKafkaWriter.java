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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.lineage.KafkaDatasetFacet;
import org.apache.flink.connector.kafka.lineage.KafkaDatasetFacetProvider;
import org.apache.flink.connector.kafka.lineage.KafkaDatasetIdentifier;
import org.apache.flink.connector.kafka.sink.internal.BackchannelFactory;
import org.apache.flink.connector.kafka.sink.internal.CheckpointTransaction;
import org.apache.flink.connector.kafka.sink.internal.FlinkKafkaInternalProducer;
import org.apache.flink.connector.kafka.sink.internal.ProducerPool;
import org.apache.flink.connector.kafka.sink.internal.ProducerPoolImpl;
import org.apache.flink.connector.kafka.sink.internal.ReadableBackchannel;
import org.apache.flink.connector.kafka.sink.internal.TransactionAbortStrategyContextImpl;
import org.apache.flink.connector.kafka.sink.internal.TransactionAbortStrategyImpl;
import org.apache.flink.connector.kafka.sink.internal.TransactionFinished;
import org.apache.flink.connector.kafka.sink.internal.TransactionNamingStrategyContextImpl;
import org.apache.flink.connector.kafka.sink.internal.TransactionNamingStrategyImpl;
import org.apache.flink.connector.kafka.sink.internal.TransactionOwnership;
import org.apache.flink.connector.kafka.sink.internal.TransactionalIdFactory;
import org.apache.flink.connector.kafka.sink.internal.TransactionalIdFactory.AttemptScope;
import org.apache.flink.connector.kafka.util.AdminUtils;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.apache.flink.util.IOUtils.closeAll;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Exactly-once Kafka writer that writes records to Kafka in transactions.
 *
 * @param <IN> The type of the input elements.
 */
class ExactlyOnceKafkaWriter<IN> extends KafkaWriter<IN> {
    private static final Logger LOG = LoggerFactory.getLogger(ExactlyOnceKafkaWriter.class);

    /**
     * Prefix for the transactional id. Must be unique across all sinks writing to the same broker.
     */
    private final String transactionalIdPrefix;

    /**
     * Prefix of the transactional ids of this execution attempt: {@link #transactionalIdPrefix}
     * followed by the restored checkpoint, the subtask index and the attempt number. No other
     * attempt that the JobManager starts has this prefix, so this writer and the writer of another
     * attempt that is alive at the same time cannot end each other's transactions by opening their
     * own.
     */
    private final String attemptScopedPrefix;

    private final int subtaskId;
    private final int attemptNumber;

    /**
     * Strategy to abort lingering transactions from previous executions during writer
     * initialization.
     */
    private final TransactionAbortStrategyImpl transactionAbortStrategy;

    /** Strategy to name transactions. */
    private final TransactionNamingStrategyImpl transactionNamingStrategy;

    private final Collection<KafkaWriterState> recoveredStates;

    /** Ids of the transactions that the recovered state hands over to the committer. */
    private final Set<String> recoveredTransactionalIds;

    private final long restoredCheckpointId;

    /**
     * The producer pool that manages all transactional producers. It keeps track of the producers
     * that have been recycled as well as producers that are currently in use (potentially forwarded
     * to committer).
     */
    private final ProducerPool producerPool;

    /**
     * Backchannel used to communicate committed transactions from the committer to this writer.
     * Establishing the channel happens during recovery. Thus, it is only safe to poll in checkpoint
     * related methods.
     */
    private final ReadableBackchannel<TransactionFinished> backchannel;

    /** The context used to name transactions. */
    private final TransactionNamingStrategyContextImpl namingContext;

    private final int totalNumberOfOwnedSubtasks;
    private final int[] ownedSubtaskIds;

    /** Lazily created admin client for listing transactions and resolving topic patterns. */
    private AdminClient adminClient;

    /**
     * Constructor creating a kafka writer.
     *
     * <p>It will throw a {@link RuntimeException} if {@link
     * KafkaRecordSerializationSchema#open(SerializationSchema.InitializationContext,
     * KafkaRecordSerializationSchema.KafkaSinkContext)} fails.
     *
     * @param deliveryGuarantee the Sink's delivery guarantee
     * @param kafkaProducerConfig the properties to configure the {@link FlinkKafkaInternalProducer}
     * @param transactionalIdPrefix used to create the transactionalIds
     * @param sinkInitContext context to provide information about the runtime environment
     * @param recordSerializer serialize to transform the incoming records to {@link ProducerRecord}
     * @param schemaContext context used to initialize the {@link KafkaRecordSerializationSchema}
     * @param recoveredStates state from an previous execution which was covered
     */
    ExactlyOnceKafkaWriter(
            DeliveryGuarantee deliveryGuarantee,
            Properties kafkaProducerConfig,
            String transactionalIdPrefix,
            WriterInitContext sinkInitContext,
            KafkaRecordSerializationSchema<IN> recordSerializer,
            SerializationSchema.InitializationContext schemaContext,
            TransactionAbortStrategyImpl transactionAbortStrategy,
            TransactionNamingStrategyImpl transactionNamingStrategy,
            Collection<KafkaWriterState> recoveredStates) {
        super(
                deliveryGuarantee,
                kafkaProducerConfig,
                sinkInitContext,
                recordSerializer,
                schemaContext);
        this.transactionalIdPrefix =
                checkNotNull(transactionalIdPrefix, "transactionalIdPrefix must not be null");
        this.transactionAbortStrategy =
                checkNotNull(transactionAbortStrategy, "transactionAbortStrategy must not be null");
        this.transactionNamingStrategy =
                checkNotNull(
                        transactionNamingStrategy, "transactionNamingStrategy must not be null");

        try {
            recordSerializer.open(schemaContext, kafkaSinkContext);
        } catch (Exception e) {
            throw new FlinkRuntimeException("Cannot initialize schema.", e);
        }

        this.recoveredStates = checkNotNull(recoveredStates, "recoveredStates");
        this.recoveredTransactionalIds =
                recoveredStates.stream()
                        .flatMap(
                                state ->
                                        state.getPrecommittedTransactionalIds().stream()
                                                .map(CheckpointTransaction::getTransactionalId))
                        .collect(Collectors.toSet());
        TaskInfo taskInfo = sinkInitContext.getTaskInfo();
        TransactionOwnership ownership = transactionNamingStrategy.getOwnership();
        this.subtaskId = taskInfo.getIndexOfThisSubtask();
        this.attemptNumber = taskInfo.getAttemptNumber();
        int parallelism = taskInfo.getNumberOfParallelSubtasks();
        this.ownedSubtaskIds =
                ownership.getOwnedSubtaskIds(subtaskId, parallelism, recoveredStates);
        this.totalNumberOfOwnedSubtasks =
                ownership.getTotalNumberOfOwnedSubtasks(subtaskId, parallelism, recoveredStates);
        initFlinkMetrics();
        restoredCheckpointId =
                sinkInitContext
                        .getRestoredCheckpointId()
                        .orElse(CheckpointIDCounter.INITIAL_CHECKPOINT_ID - 1);
        this.attemptScopedPrefix =
                TransactionalIdFactory.buildAttemptScopedPrefix(
                        transactionalIdPrefix, restoredCheckpointId, subtaskId, attemptNumber);
        this.producerPool =
                new ProducerPoolImpl(
                        kafkaProducerConfig,
                        this::initKafkaMetrics,
                        recoveredStates.stream()
                                .flatMap(r -> r.getPrecommittedTransactionalIds().stream())
                                .collect(Collectors.toList()));
        this.backchannel =
                BackchannelFactory.getInstance()
                        .getReadableBackchannel(subtaskId, attemptNumber, transactionalIdPrefix);
        this.namingContext =
                new TransactionNamingStrategyContextImpl(
                        attemptScopedPrefix,
                        this.ownedSubtaskIds[0],
                        restoredCheckpointId,
                        producerPool);
    }

    @Override
    public void initialize() {
        // Workaround for FLINK-37612: ensure that we are not leaking producers
        try {
            abortLingeringTransactions(
                    checkNotNull(recoveredStates, "recoveredStates"), restoredCheckpointId + 1);
            this.currentProducer = startTransaction(restoredCheckpointId + 1);
        } catch (Throwable t) {
            try {
                close();
            } catch (Exception e) {
                t.addSuppressed(e);
            }
            throw t;
        }
    }

    private FlinkKafkaInternalProducer<byte[], byte[]> startTransaction(long checkpointId) {
        namingContext.setNextCheckpointId(checkpointId);
        namingContext.setOngoingTransactions(
                producerPool.getOngoingTransactions().stream()
                        .map(CheckpointTransaction::getTransactionalId)
                        .collect(Collectors.toSet()));
        FlinkKafkaInternalProducer<byte[], byte[]> producer =
                transactionNamingStrategy.getTransactionalProducer(namingContext);
        namingContext.setLastCheckpointId(checkpointId);
        producer.beginTransaction();
        return producer;
    }

    @Override
    public Collection<KafkaCommittable> prepareCommit() {
        // only return a KafkaCommittable if the current transaction has been written some data
        if (currentProducer.hasRecordsInTransaction()) {
            KafkaCommittable committable = KafkaCommittable.of(currentProducer);
            LOG.debug("Prepare {}.", committable);
            currentProducer.precommitTransaction();
            return Collections.singletonList(committable);
        }

        // otherwise, we recycle the producer (the pool will reset the transaction state)
        producerPool.recycle(currentProducer);
        return Collections.emptyList();
    }

    @Override
    public List<KafkaWriterState> snapshotState(long checkpointId) throws IOException {
        // recycle committed producers
        TransactionFinished finishedTransaction;
        while ((finishedTransaction = backchannel.poll()) != null) {
            final String transactionalId = finishedTransaction.getTransactionId();
            producerPool.recycleByTransactionId(transactionalId, finishedTransaction.isSuccess());
            if (!finishedTransaction.isSuccess()
                    && recoveredTransactionalIds.contains(transactionalId)) {
                // The committer could not commit a transaction of the recovered state, so another
                // transaction may be open under its id: the attempt that wrote the state may have
                // used the id again for a transaction that no checkpoint covers. The cleanup at
                // the start spared the id for the committer, and this writer does not use ids of
                // that attempt, so the transaction has to be aborted here.
                abortTransaction(transactionalId);
            }
        }
        // persist the ongoing transactions into the state; these will not be aborted on restart
        Collection<CheckpointTransaction> ongoingTransactions =
                producerPool.getOngoingTransactions();
        currentProducer = startTransaction(checkpointId + 1);
        return createSnapshots(ongoingTransactions);
    }

    private List<KafkaWriterState> createSnapshots(
            Collection<CheckpointTransaction> ongoingTransactions) {
        List<KafkaWriterState> states = new ArrayList<>();
        int[] subtaskIds = this.ownedSubtaskIds;
        for (int index = 0; index < subtaskIds.length; index++) {
            int ownedSubtask = subtaskIds[index];
            states.add(
                    new KafkaWriterState(
                            // the writer that restores this state finds the transactions of this
                            // attempt under this prefix
                            attemptScopedPrefix,
                            ownedSubtask,
                            totalNumberOfOwnedSubtasks,
                            transactionNamingStrategy.getOwnership(),
                            // new transactions are only created with the first owned subtask id
                            index == 0 ? ongoingTransactions : List.of()));
        }
        LOG.debug("Snapshotting state {}", states);
        return states;
    }

    @Override
    public void close() throws Exception {
        markClosed();
        closeAll(
                this::abortCurrentProducer,
                () -> closeAll(producerPool),
                backchannel,
                super::close,
                adminClient);
    }

    private void abortCurrentProducer() {
        // Abort only if the transaction is known to the broker (at least one record sent).
        // Producer may be in precommitted state if we run in batch; aborting would mean data loss.
        // Note that this may leave the transaction open if an error happens in streaming between
        // #prepareCommit and #snapshotState. However, aborting here is best effort anyways and
        // recovery will cleanup the transaction.
        if (currentProducer.hasRecordsInTransaction()) {
            try {
                currentProducer.abortTransaction();
            } catch (ProducerFencedException e) {
                LOG.debug(
                        "Producer {} fenced while aborting", currentProducer.getTransactionalId());
            }
        }
    }

    @VisibleForTesting
    ProducerPool getProducerPool() {
        return producerPool;
    }

    @Nullable
    Admin getAdminClient() {
        return adminClient;
    }

    @VisibleForTesting
    public String getTransactionalIdPrefix() {
        return transactionalIdPrefix;
    }

    private void abortLingeringTransactions(
            Collection<KafkaWriterState> recoveredStates, long startCheckpointId) {
        LOG.info(
                "Aborting lingering transactions from previous execution. Recovered states: {}.",
                recoveredStates);
        // prefixes written by versions that did not scope transactional ids to an attempt
        final Set<String> unscopedPrefixes = new LinkedHashSet<>();
        final Map<String, Set<Integer>> restoredAttemptScopedPrefixes = new LinkedHashMap<>();
        // The checkpoint was written by an attempt, so the attempts of this subtask up to that
        // one cannot have restored it.
        int firstAttemptOnRestoredCheckpoint = 0;
        for (KafkaWriterState state : recoveredStates) {
            final String restoredPrefix = state.getTransactionalIdPrefix();
            final Optional<AttemptScope> scope =
                    TransactionalIdFactory.extractAttemptScope(restoredPrefix);
            if (scope.isPresent()) {
                restoredAttemptScopedPrefixes
                        .computeIfAbsent(restoredPrefix, prefix -> new TreeSet<>())
                        .add(state.getOwnedSubtaskId());
                if (isEarlierAttemptOfThisSubtask(scope.get())) {
                    firstAttemptOnRestoredCheckpoint =
                            Math.max(
                                    firstAttemptOnRestoredCheckpoint,
                                    scope.get().getAttemptNumber() + 1);
                }
            } else {
                unscopedPrefixes.add(restoredPrefix);
            }
            final String configuredPrefixOfState =
                    scope.map(AttemptScope::getTransactionalIdPrefix).orElse(restoredPrefix);
            if (!configuredPrefixOfState.equals(transactionalIdPrefix)) {
                LOG.warn(
                        "Transactional id prefix from previous execution {} has changed to {}.",
                        configuredPrefixOfState,
                        transactionalIdPrefix);
            }
        }
        final List<String> prefixesOfEarlierAttempts = new ArrayList<>();
        for (int attempt = attemptNumber - 1;
                attempt >= firstAttemptOnRestoredCheckpoint;
                attempt--) {
            prefixesOfEarlierAttempts.add(
                    TransactionalIdFactory.buildAttemptScopedPrefix(
                            transactionalIdPrefix, restoredCheckpointId, subtaskId, attempt));
        }

        LOG.info(
                "Aborting lingering transactions of this attempt's prefix {}, of {} earlier attempts on the same checkpoint, and of the recovered prefixes {} and {} using {}",
                attemptScopedPrefix,
                prefixesOfEarlierAttempts.size(),
                restoredAttemptScopedPrefixes,
                unscopedPrefixes,
                transactionAbortStrategy);
        TransactionAbortStrategyContextImpl context =
                getTransactionAbortStrategyContext(
                        startCheckpointId,
                        unscopedPrefixes,
                        prefixesOfEarlierAttempts,
                        restoredAttemptScopedPrefixes);
        transactionAbortStrategy.abortTransactions(context);
    }

    private boolean isEarlierAttemptOfThisSubtask(AttemptScope scope) {
        // a JobManager that takes over the job counts the attempts from 0 again, so the attempt
        // of the state can also have a higher number than this one
        return scope.getTransactionalIdPrefix().equals(transactionalIdPrefix)
                && scope.getSubtaskIndex() == subtaskId
                && scope.getAttemptNumber() < attemptNumber;
    }

    private TransactionAbortStrategyContextImpl getTransactionAbortStrategyContext(
            long startCheckpointId,
            Set<String> unscopedPrefixes,
            List<String> prefixesOfEarlierAttempts,
            Map<String, Set<Integer>> restoredAttemptScopedPrefixes) {
        return new TransactionAbortStrategyContextImpl(
                this::getTopicNames,
                kafkaSinkContext.getParallelInstanceId(),
                kafkaSinkContext.getNumberOfParallelInstances(),
                ownedSubtaskIds,
                totalNumberOfOwnedSubtasks,
                unscopedPrefixes,
                attemptScopedPrefix,
                prefixesOfEarlierAttempts,
                restoredAttemptScopedPrefixes,
                startCheckpointId,
                this::abortTransaction,
                this::getOrCreateAdminClient,
                recoveredTransactionalIds);
    }

    /**
     * Aborts the transaction that is open under the given id, if any.
     *
     * @return the epoch of the producer after the abort; 0 if the id was unused before
     */
    private short abortTransaction(String transactionalId) {
        // getTransactionalProducer already calls initTransactions, which cancels the transaction
        FlinkKafkaInternalProducer<byte[], byte[]> producer =
                producerPool.getTransactionalProducer(transactionalId, 0);
        LOG.debug("Aborting transaction {}", transactionalId);
        producer.flush();
        short epoch = producer.getEpoch();
        producerPool.recycle(producer);
        return epoch;
    }

    private Collection<String> getTopicNames() {
        KafkaDatasetIdentifier identifier =
                getDatasetIdentifier()
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "The record serializer does not expose a static list of target topics."));
        if (identifier.getTopics() != null) {
            return identifier.getTopics();
        }
        return AdminUtils.getTopicsByPattern(
                getOrCreateAdminClient(), identifier.getTopicPattern());
    }

    private Optional<KafkaDatasetIdentifier> getDatasetIdentifier() {
        if (recordSerializer instanceof KafkaDatasetFacetProvider) {
            Optional<KafkaDatasetFacet> kafkaDatasetFacet =
                    ((KafkaDatasetFacetProvider) recordSerializer).getKafkaDatasetFacet();

            return kafkaDatasetFacet.map(KafkaDatasetFacet::getTopicIdentifier);
        }
        return Optional.empty();
    }

    private Admin getOrCreateAdminClient() {
        if (adminClient == null) {
            adminClient = AdminClient.create(kafkaProducerConfig);
        }
        return adminClient;
    }
}
