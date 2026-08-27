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

package org.apache.flink.streaming.connectors.kafka.internals.metrics;

import org.apache.flink.metrics.Gauge;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;

/** Unit tests for {@link KafkaMetricMutableWrapper}. */
class KafkaMetricMutableWrapperTest {

    // The clients are never used to communicate with a broker: only their locally registered
    // metrics are wrapped and read, so a syntactically valid address is all that is needed.
    private static final String DUMMY_BOOTSTRAP_SERVERS = "localhost:12345";

    @Test
    void testOnlyMeasurableMetricsAreRegisteredWithMutableWrapper() {
        testOnlyMeasurableMetricsAreRegistered(KafkaMetricMutableWrapper::new);
    }

    private static void testOnlyMeasurableMetricsAreRegistered(
            Function<Metric, Gauge<Double>> wrapperFactory) {
        final Collection<Gauge<Double>> metricWrappers = new ArrayList<>();
        try (final KafkaConsumer<?, ?> consumer =
                        new KafkaConsumer<>(getKafkaClientConfiguration());
                final KafkaProducer<?, ?> producer =
                        new KafkaProducer<>(getKafkaClientConfiguration())) {
            Stream.concat(
                            consumer.metrics().values().stream(),
                            producer.metrics().values().stream())
                    .map(wrapperFactory::apply)
                    .forEach(metricWrappers::add);

            // Ensure that all values are accessible and return valid double values
            metricWrappers.forEach(Gauge::getValue);
        }
    }

    private static Properties getKafkaClientConfiguration() {
        final Properties standardProps = new Properties();
        standardProps.put("bootstrap.servers", DUMMY_BOOTSTRAP_SERVERS);
        standardProps.put("group.id", UUID.randomUUID().toString());
        standardProps.put("enable.auto.commit", false);
        standardProps.put("key.deserializer", ByteArrayDeserializer.class.getName());
        standardProps.put("value.deserializer", ByteArrayDeserializer.class.getName());
        standardProps.put("key.serializer", ByteArraySerializer.class.getName());
        standardProps.put("value.serializer", ByteArraySerializer.class.getName());
        standardProps.put("auto.offset.reset", "earliest");
        standardProps.put("max.partition.fetch.bytes", 256);
        return standardProps;
    }
}
