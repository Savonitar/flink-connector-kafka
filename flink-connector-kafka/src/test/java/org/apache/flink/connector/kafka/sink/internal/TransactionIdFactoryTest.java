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

import org.apache.flink.connector.kafka.sink.internal.TransactionalIdFactory.AttemptScope;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TransactionalIdFactory}. */
class TransactionIdFactoryTest {

    @Test
    void testBuildTransactionalId() {
        final String expected = "prefix-1-2";
        assertThat(TransactionalIdFactory.buildTransactionalId("prefix", 1, 2L))
                .isEqualTo(expected);
    }

    @Test
    void testExtractSubtaskId() {
        final String transactionalId = "prefix-1-2";
        assertThat(TransactionalIdFactory.extractSubtaskId(transactionalId)).isEqualTo(1);
    }

    @Test
    void testExtractPrefix() {
        final String transactionalId = "prefix-1-2";
        assertThat(TransactionalIdFactory.extractPrefix(transactionalId)).isEqualTo("prefix");
    }

    @Test
    void testBuildAttemptScopedPrefix() {
        assertThat(TransactionalIdFactory.buildAttemptScopedPrefix("prefix", 7L, 1, 3))
                .isEqualTo("prefix-7_1_3");
    }

    @Test
    void testAttemptScopedPrefixKeepsFormatOfTransactionalId() {
        final String scopedPrefix =
                TransactionalIdFactory.buildAttemptScopedPrefix("my-prefix", 7L, 1, 3);
        final String transactionalId =
                TransactionalIdFactory.buildTransactionalId(scopedPrefix, 4, 8L);

        assertThat(TransactionalIdFactory.extractSubtaskId(transactionalId)).isEqualTo(4);
        assertThat(TransactionalIdFactory.extractPrefix(transactionalId)).isEqualTo(scopedPrefix);
        assertThat(TransactionalIdFactory.findPrefix(transactionalId)).hasValue(scopedPrefix);
    }

    @Test
    void testExtractAttemptScope() {
        final Optional<AttemptScope> scope =
                TransactionalIdFactory.extractAttemptScope("my-prefix-7_1_3");

        assertThat(scope).isPresent();
        assertThat(scope.get().getTransactionalIdPrefix()).isEqualTo("my-prefix");
        assertThat(scope.get().getRestoredCheckpointId()).isEqualTo(7L);
        assertThat(scope.get().getSubtaskIndex()).isEqualTo(1);
        assertThat(scope.get().getAttemptNumber()).isEqualTo(3);
    }

    @ParameterizedTest
    @ValueSource(strings = {"prefix", "my-prefix-7", "prefix-7_1", "prefix-7_1_x", "prefix7_1_3"})
    void testExtractAttemptScopeOfPrefixWithoutScope(String prefix) {
        assertThat(TransactionalIdFactory.extractAttemptScope(prefix)).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"other-application", "prefix", "prefix-1", "prefix-a-2", "prefix-1-b"})
    void testFindPrefixOfForeignTransactionalId(String transactionalId) {
        assertThat(TransactionalIdFactory.findPrefix(transactionalId)).isEmpty();
    }
}
