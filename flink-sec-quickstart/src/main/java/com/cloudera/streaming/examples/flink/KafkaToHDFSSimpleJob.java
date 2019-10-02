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

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class KafkaToHDFSSimpleJob {

    private static Logger LOG = LoggerFactory.getLogger(KafkaToHDFSSimpleJob.class);
    private static final String KAFKA_PREFIX = "kafka.";

    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        final String P_PROPERTIES_FILE = params.get("properties.file");

        if (P_PROPERTIES_FILE != null) {
            params = ParameterTool.fromPropertiesFile(P_PROPERTIES_FILE).mergeWith(params);
        }

        LOG.info("### Job parameters:");
        for (String key : params.getProperties().stringPropertyNames()) {
            LOG.info("Job param {}={}", key, params.get(key));
        }

        final String P_KAFKA_TOPIC = params.get("kafkaTopic");
        final String P_FS_OUTPUT = params.get("hdfsOutput");

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        for (String key : params.getProperties().stringPropertyNames()) {
            if (key.startsWith(KAFKA_PREFIX)) {
                properties.setProperty(key.substring(KAFKA_PREFIX.length()), params.get(key));
            }
        }

        LOG.info("### Kafka parameters:");
        for (String key : properties.stringPropertyNames()) {
            LOG.info("Kafka param {}={}", key, params.get(key));
        }

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(P_KAFKA_TOPIC, new SimpleStringSchema(), properties);
        DataStream<String> source = env.addSource(consumer).name("Kafka Source");

        source.print();

        final StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(new Path(P_FS_OUTPUT), new SimpleStringEncoder<String>("UTF-8"))
                .build();

        source.addSink(sink).name("FS Sink");


        env.execute("Flink Streaming Secured Job Sample");
    }
}
