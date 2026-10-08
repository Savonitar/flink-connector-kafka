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

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.annotation.PublicEvolving;

/**
 * The method used to abort transactions. It does not choose which transactions are aborted; it only
 * chooses the underlying method used to abort them.
 */
@PublicEvolving
public enum TransactionAbortMethod {
    /**
     * Fences each transactional id through a transactional {@code KafkaProducer} by calling {@code
     * KafkaProducer#initTransactions()}.
     */
    PRODUCER_INIT_TRANSACTIONS,

    /**
     * Fences each transactional id through one shared {@code Admin} client by calling {@code
     * Admin#fenceProducers}. No producer is created or reconfigured for aborting, and the new epoch
     * is taken from the coordinator's response instead of from producer internals.
     *
     * <p>Requires the same permissions on the transactional ids as {@link
     * #PRODUCER_INIT_TRANSACTIONS}.
     */
    ADMIN_FENCE_PRODUCERS;

    public static final TransactionAbortMethod DEFAULT = PRODUCER_INIT_TRANSACTIONS;
}
