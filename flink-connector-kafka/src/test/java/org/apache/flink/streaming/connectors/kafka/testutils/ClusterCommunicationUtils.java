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

package org.apache.flink.streaming.connectors.kafka.testutils;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.function.ThrowingRunnable;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** Utilities for communicating with a cluster through a {@link ClusterClient}. */
public class ClusterCommunicationUtils {

    public static void waitUntilJobIsRunning(ClusterClient<?> client) throws Exception {
        while (getRunningJobs(client).isEmpty()) {
            Thread.sleep(50);
        }
    }

    public static void waitUntilNoJobIsRunning(ClusterClient<?> client) throws Exception {
        while (!getRunningJobs(client).isEmpty()) {
            Thread.sleep(50);
        }
    }

    public static List<JobID> getRunningJobs(ClusterClient<?> client) throws Exception {
        Collection<JobStatusMessage> statusMessages = client.listJobs().get();
        return statusMessages.stream()
                .filter(status -> !status.getJobState().isGloballyTerminalState())
                .map(JobStatusMessage::getJobId)
                .collect(Collectors.toList());
    }

    public static void cancelJobAfterSubmission(
            ClusterClient<?> client,
            JobID jobId,
            CompletableFuture<JobID> submission,
            Duration timeout)
            throws Exception {
        // A timeout waiting for the acknowledgement does not cancel the submission. The callback
        // stays registered even if cleanup times out, and uses the job graph's ID if the ack fails.
        submission
                .handle((id, error) -> null)
                .thenRunAsync(
                        ThrowingRunnable.unchecked(
                                () -> cancelJobIfRunning(client, jobId, timeout)))
                .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    private static void cancelJobIfRunning(ClusterClient<?> client, JobID jobId, Duration timeout)
            throws Exception {
        final long timeoutMillis = timeout.toMillis();
        try {
            client.cancel(jobId).get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            if (ExceptionUtils.findThrowable(e, FlinkJobNotFoundException.class).isPresent()) {
                return;
            }
            // A cancellation can be rejected if the job has already finished.
            if (!client.getJobStatus(jobId)
                    .get(timeoutMillis, TimeUnit.MILLISECONDS)
                    .isGloballyTerminalState()) {
                throw e;
            }
        }
        client.requestJobResult(jobId).get(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    private ClusterCommunicationUtils() {}
}
