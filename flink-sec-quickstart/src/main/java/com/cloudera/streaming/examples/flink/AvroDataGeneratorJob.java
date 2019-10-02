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

package com.cloudera.streaming.examples.flink;

import com.cloudera.streaming.examples.flink.data.Message;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.formats.avro.registry.cloudera.SchemaRegistrySerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;


public class AvroDataGeneratorJob {

    private static Logger LOG = LoggerFactory.getLogger(AvroDataGeneratorJob.class);
    private static final String KAFKA_PREFIX = "kafka.";
    private static final String K_KAFKA_TOPIC = "kafkaTopic";

    private static final String K_SCHEMA_REG_URL = "schema.registry.url";
    private static final String K_SCHEMA_REG_SSL_CLIENT_KEY = "schema.registry.client.ssl";
    private static final String K_TRUSTSTORE_PATH = "trustStorePath";
    private static final String K_TRUSTSTORE_PASSWORD = "trustStorePassword";
    private static final String K_KEYSTORE_PASSWORD = "keyStorePassword";


    public static void main(String[] args) throws Exception {

        // Processing job properties
        ParameterTool params = ParameterTool.fromArgs(args);
        final String P_PROPERTIES_FILE = params.get("properties.file");

        if (P_PROPERTIES_FILE != null) {
            params = ParameterTool.fromPropertiesFile(P_PROPERTIES_FILE).mergeWith(params);
        }

        LOG.info("### Job parameters:");
        for (String key : params.getProperties().stringPropertyNames()) {
            LOG.info("Job Param: {}={}", key, params.get(key));
        }

        final String P_KAFKA_TOPIC = params.get(K_KAFKA_TOPIC);

        //Setting up schema registry client
        Map<String, String> sslClientConfig = new HashMap<>();
        sslClientConfig.put(K_TRUSTSTORE_PATH, params.get(K_SCHEMA_REG_SSL_CLIENT_KEY + "." + K_TRUSTSTORE_PATH));
        sslClientConfig.put(K_TRUSTSTORE_PASSWORD, params.get(K_SCHEMA_REG_SSL_CLIENT_KEY + "." + K_TRUSTSTORE_PASSWORD));
        sslClientConfig.put(K_KEYSTORE_PASSWORD, ""); //ugly hack needed for SchemaRegistryClient

        Map<String, Object> schemaRegistryConf = new HashMap<>();
        schemaRegistryConf.put(K_SCHEMA_REG_URL, params.get(K_SCHEMA_REG_URL));
        schemaRegistryConf.put(K_SCHEMA_REG_SSL_CLIENT_KEY, sslClientConfig);

        LOG.info("### Schema Registry parameters:");
        for (String key : schemaRegistryConf.keySet()) {
            LOG.info("Schema Registry param: {}={}", key, schemaRegistryConf.get(key));
        }

        KafkaSerializationSchema<Message> schema = SchemaRegistrySerializationSchema.<Message>
                builder(P_KAFKA_TOPIC)
                .setConfig(schemaRegistryConf)
                .setKey(Message::getId)
                .build();

        Properties properties = new Properties();
        for (String key : params.getProperties().stringPropertyNames()) {
            if (key.startsWith(KAFKA_PREFIX)) {
                properties.setProperty(key.substring(KAFKA_PREFIX.length()), params.get(key));
            }
        }

        LOG.info("### Kafka parameters:");
        for (String key : properties.stringPropertyNames()) {
            LOG.info("Kafka param: {}={}", key, properties.get(key));
        }

        FlinkKafkaProducer<Message> kafkaSink = new FlinkKafkaProducer<>(
                "default", (KafkaSerializationSchema<Message>) schema,
                properties, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Message> input = env.addSource(new DataGeneratorSource()).name("Data Generator Source");

        input.addSink(kafkaSink).name("Kafka Sink");
        input.print();
        env.execute("Data Generator Job");
    }

    static class DataGeneratorSource implements ParallelSourceFunction<Message> {

        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<Message> ctx) throws Exception {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();
            while (isRunning) {
                ctx.collect(new Message(UUID.randomUUID().toString() + " - " + LocalDateTime.now().toString(),
                        RandomStringUtils.randomAlphabetic(10),
                        RandomStringUtils.randomAlphabetic(100)));

                Thread.sleep(Math.abs(rnd.nextInt()) % 1000);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;

        }
    }
}
