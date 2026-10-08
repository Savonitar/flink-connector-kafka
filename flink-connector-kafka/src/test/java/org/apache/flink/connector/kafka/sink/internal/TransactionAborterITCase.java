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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.TransactionAbortMethod;
import org.apache.flink.connector.kafka.testutils.TestKafkaContainer;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TransactionDescription;
import org.apache.kafka.clients.admin.TransactionState;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.annotation.Nullable;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.flink.connector.kafka.testutils.KafkaUtil.checkProducerLeak;
import static org.apache.flink.connector.kafka.testutils.KafkaUtil.createKafkaContainer;
import static org.apache.flink.connector.kafka.testutils.KafkaUtil.createNewTopicAndWaitForPartitionAssignment;
import static org.apache.flink.connector.kafka.testutils.KafkaUtil.drainAllRecordsFromTopic;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Contract of {@link TransactionAborter} against a real broker, for {@link
 * AdminFencingTransactionAborter} and {@link ProducerFencingTransactionAborter}.
 */
@Testcontainers
class TransactionAborterITCase {

    private static final String AFTER_FENCE = "after-fence";

    @RegisterExtension
    public static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(1)
                            .setConfiguration(new Configuration())
                            .build());

    @Container
    public static final TestKafkaContainer KAFKA_CONTAINER =
            createKafkaContainer(TransactionAborterITCase.class);

    private String topic;
    private String transactionalId;
    private Admin admin;
    @Nullable private ProducerPoolImpl producerPool;

    @BeforeEach
    void setUp(TestInfo testInfo) {
        final String methodName = testInfo.getTestMethod().map(Method::getName).orElse("unknown");
        // one topic and transactional id per parameterized row
        final String name = methodName + "_" + testInfo.getDisplayName().replaceAll("\\W", "");
        topic = name;
        transactionalId = name + "-transactional-id";
        createNewTopicAndWaitForPartitionAssignment(topic, 1, (short) 1, getClientConfig());
        admin = AdminClient.create(getClientConfig());
    }

    @AfterEach
    void tearDown() throws Exception {
        if (producerPool != null) {
            producerPool.close();
        }
        admin.close();
        checkProducerLeak();
    }

    @ParameterizedTest
    @EnumSource(TransactionAbortMethod.class)
    void testUnknownTransactionalIdReportsEpochZero(TransactionAbortMethod method) {
        assertThat(aborter(method).abortTransaction(transactionalId)).isZero();
    }

    @ParameterizedTest
    @EnumSource(TransactionAbortMethod.class)
    void testFencingAbortsOpenTransaction(TransactionAbortMethod method) throws Exception {
        final TransactionAborter aborter = aborter(method);
        try (FlinkKafkaInternalProducer<byte[], byte[]> producer =
                new FlinkKafkaInternalProducer<>(getProducerConfig(), transactionalId)) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(topic, "1".getBytes()));
            producer.flush();
            final short epochBeforeFencing = producer.getEpoch();
            assertThat(describeTransaction().state()).isEqualTo(TransactionState.ONGOING);

            assertThat(aborter.abortTransaction(transactionalId)).isGreaterThan(epochBeforeFencing);
            final TransactionDescription afterFencing = describeTransaction();
            assertThat(afterFencing.state())
                    .isIn(TransactionState.EMPTY, TransactionState.COMPLETE_ABORT);
            assertThat(afterFencing.producerEpoch()).isGreaterThan(epochBeforeFencing);

            assertThatThrownBy(producer::commitTransaction)
                    .isInstanceOfAny(
                            ProducerFencedException.class, InvalidProducerEpochException.class);
        }

        // read_committed stops at the last stable offset, which an open transaction holds back
        produceNonTransactional(AFTER_FENCE);
        assertThat(readCommitted()).containsExactly(AFTER_FENCE);
    }

    /**
     * The admin path must be interchangeable with the producer path: both report the epoch that the
     * coordinator assigned to the transactional id, so a probing strategy stops at the same
     * transactional id regardless of the method.
     */
    @ParameterizedTest
    @EnumSource(TransactionAbortMethod.class)
    void testReportsEpochAssignedByCoordinator(TransactionAbortMethod method) {
        final TransactionAborter aborter = aborter(method);
        try (FlinkKafkaInternalProducer<byte[], byte[]> producer =
                new FlinkKafkaInternalProducer<>(getProducerConfig(), transactionalId)) {
            producer.initTransactions();
            assertThat(producer.getEpoch()).isZero();
        }

        final int epochAfterFencing = aborter.abortTransaction(transactionalId);
        assertThat(epochAfterFencing).isEqualTo(1);

        try (FlinkKafkaInternalProducer<byte[], byte[]> producer =
                new FlinkKafkaInternalProducer<>(getProducerConfig(), transactionalId)) {
            producer.initTransactions();
            assertThat((int) producer.getEpoch()).isEqualTo(epochAfterFencing + 1);
        }
    }

    /**
     * kafka-clients sends the fence timeout as the transaction timeout of the {@code
     * InitProducerId} request, so the coordinator reports it for the fenced transactional id.
     */
    @Test
    void testAdminFenceSendsTimeoutAsTransactionTimeout() throws Exception {
        final int timeoutMs = 12_345;
        new AdminFencingTransactionAborter(() -> admin, timeoutMs)
                .abortTransaction(transactionalId);

        assertThat(describeTransaction().transactionTimeoutMs()).isEqualTo(timeoutMs);
    }

    private TransactionDescription describeTransaction() throws Exception {
        return admin.describeTransactions(List.of(transactionalId))
                .description(transactionalId)
                .get();
    }

    private void produceNonTransactional(String value) throws Exception {
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(getProducerConfig())) {
            producer.send(new ProducerRecord<>(topic, value.getBytes(StandardCharsets.UTF_8)))
                    .get();
        }
    }

    private List<String> readCommitted() {
        return drainAllRecordsFromTopic(topic, getClientConfig(), true).stream()
                .map(record -> new String(record.value(), StandardCharsets.UTF_8))
                .collect(Collectors.toList());
    }

    private TransactionAborter aborter(TransactionAbortMethod method) {
        switch (method) {
            case ADMIN_FENCE_PRODUCERS:
                return new AdminFencingTransactionAborter(
                        () -> admin,
                        AdminFencingTransactionAborter.fenceTimeoutMs(getProducerConfig()));
            case PRODUCER_INIT_TRANSACTIONS:
                producerPool = new ProducerPoolImpl(getProducerConfig(), producer -> {}, List.of());
                return new ProducerFencingTransactionAborter(producerPool);
            default:
                throw new IllegalArgumentException("Unknown transaction abort method " + method);
        }
    }

    private static Properties getClientConfig() {
        final Properties properties = new Properties();
        properties.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "admin-fencing-tests");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }

    private static Properties getProducerConfig() {
        final Properties properties = new Properties();
        properties.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
        properties.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return properties;
    }
}
