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

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.kafka.sink.TransactionAbortMethod;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.FenceProducersOptions;
import org.apache.kafka.clients.admin.FenceProducersResult;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Fences a transactional id through {@link Admin#fenceProducers}.
 *
 * <p>The returned epoch is the one assigned by the coordinator. It is {@code 0} for a transactional
 * id the coordinator has never seen. Each fence times out after {@code timeoutMs}, see {@link
 * #fenceTimeoutMs(Properties)}.
 */
@Internal
public class AdminFencingTransactionAborter implements TransactionAborter {

    private final Supplier<Admin> adminSupplier;
    private final int timeoutMs;

    public AdminFencingTransactionAborter(Supplier<Admin> adminSupplier, int timeoutMs) {
        this.adminSupplier = checkNotNull(adminSupplier, "adminSupplier must not be null");
        this.timeoutMs = timeoutMs;
    }

    /**
     * Returns the fence timeout for producers configured by {@code producerConfig}: their {@code
     * max.block.ms}, which bounds the same fence on the producer path, capped by their {@code
     * transaction.timeout.ms}. Missing values fall back to the producer defaults.
     *
     * <p>kafka-clients also sends the fence timeout as the transaction timeout of the {@code
     * InitProducerId} request. The broker rejects it above {@code transaction.max.timeout.ms}, a
     * check that the producers' own transaction timeout must pass anyway. A non-positive
     * transaction timeout is no cap: it would time out every fence at once, while the producers'
     * {@code initTransactions} rejects it with a clear error.
     */
    public static int fenceTimeoutMs(Properties producerConfig) {
        final long maxBlockMs =
                (Long)
                        parseOrDefault(
                                producerConfig,
                                ProducerConfig.MAX_BLOCK_MS_CONFIG,
                                ConfigDef.Type.LONG);
        final int transactionTimeoutMs =
                (Integer)
                        parseOrDefault(
                                producerConfig,
                                ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,
                                ConfigDef.Type.INT);
        return (int)
                Math.min(
                        maxBlockMs,
                        transactionTimeoutMs > 0 ? transactionTimeoutMs : Integer.MAX_VALUE);
    }

    private static Object parseOrDefault(
            Properties producerConfig, String name, ConfigDef.Type type) {
        final Object configured = producerConfig.get(name);
        return ConfigDef.parseType(
                name,
                configured != null
                        ? configured
                        : ProducerConfig.configDef().defaultValues().get(name),
                type);
    }

    @Override
    public int abortTransaction(String transactionalId) {
        final FenceProducersResult result =
                adminSupplier
                        .get()
                        .fenceProducers(
                                Collections.singletonList(transactionalId),
                                new FenceProducersOptions().timeoutMs(timeoutMs));
        try {
            return result.epochId(transactionalId).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FlinkRuntimeException(
                    "Interrupted while fencing transactional id " + transactionalId, e);
        } catch (ExecutionException e) {
            throw new FlinkRuntimeException(
                    "Failed to fence transactional id " + transactionalId,
                    e.getCause() != null ? e.getCause() : e);
        }
    }

    @Override
    public TransactionAbortMethod methodName() {
        return TransactionAbortMethod.ADMIN_FENCE_PRODUCERS;
    }
}
