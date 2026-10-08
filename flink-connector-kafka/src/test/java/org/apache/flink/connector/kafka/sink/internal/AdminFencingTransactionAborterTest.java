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

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Properties;
import java.util.stream.Stream;

import static org.apache.flink.connector.kafka.sink.internal.AdminFencingTransactionAborter.fenceTimeoutMs;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link AdminFencingTransactionAborter#fenceTimeoutMs}. {@code TransactionAborterITCase}
 * checks against a broker that the timeout reaches the fence request.
 */
class AdminFencingTransactionAborterTest {

    static Stream<Arguments> fenceTimeouts() {
        return Stream.of(
                // max.block.ms, transaction.timeout.ms, expected fence timeout
                Arguments.of(1234L, 5678, 1234),
                Arguments.of(5678L, 1234, 1234),
                Arguments.of("1234", "5678", 1234),
                Arguments.of(Long.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE),
                // a non-positive transaction timeout is no cap
                Arguments.of(1234L, 0, 1234),
                Arguments.of(1234L, -1, 1234),
                Arguments.of(Long.MAX_VALUE, -1, Integer.MAX_VALUE));
    }

    @ParameterizedTest
    @MethodSource("fenceTimeouts")
    void testMaxBlockMsCappedByTransactionTimeout(
            Object maxBlockMs, Object transactionTimeoutMs, int expected) {
        final Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMs);
        producerConfig.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, transactionTimeoutMs);

        assertThat(fenceTimeoutMs(producerConfig)).isEqualTo(expected);
    }

    @Test
    void testFallsBackToProducerDefaults() {
        final Properties onlyMaxBlockMs = new Properties();
        onlyMaxBlockMs.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.MAX_VALUE);
        final Properties onlyTransactionTimeout = new Properties();
        onlyTransactionTimeout.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, Integer.MAX_VALUE);

        assertThat(fenceTimeoutMs(onlyMaxBlockMs))
                .isEqualTo(producerDefault(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG));
        assertThat(fenceTimeoutMs(onlyTransactionTimeout))
                .isEqualTo(producerDefault(ProducerConfig.MAX_BLOCK_MS_CONFIG));
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                ProducerConfig.MAX_BLOCK_MS_CONFIG,
                ProducerConfig.TRANSACTION_TIMEOUT_CONFIG
            })
    void testRejectsNonNumericValue(String name) {
        final Properties producerConfig = new Properties();
        producerConfig.put(name, "not-a-number");

        assertThatThrownBy(() -> fenceTimeoutMs(producerConfig))
                .isInstanceOf(ConfigException.class);
    }

    private static int producerDefault(String name) {
        return ((Number) ProducerConfig.configDef().defaultValues().get(name)).intValue();
    }
}
