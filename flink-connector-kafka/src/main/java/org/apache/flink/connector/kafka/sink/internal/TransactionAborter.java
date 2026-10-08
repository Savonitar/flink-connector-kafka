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

/**
 * Aborts a possibly lingering transaction by fencing its transactional id. The abort strategies
 * decide which transactional ids to fence.
 */
@Internal
public interface TransactionAborter {

    /**
     * Fences the transactional id, aborting any transaction still open under it, and returns the
     * producer epoch that the coordinator assigned. An epoch of 0 means that the coordinator has
     * never seen the transactional id.
     */
    int abortTransaction(String transactionalId);

    /** Returns the abort method this aborter implements, for logging. */
    TransactionAbortMethod methodName();
}
