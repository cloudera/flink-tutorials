/*
 * Licensed to Cloudera, Inc. under one
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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;

import static com.cloudera.streaming.examples.flink.Constants.K_BOOTSTRAP_SERVERS;
import static com.cloudera.streaming.examples.flink.Constants.K_HDFS_OUTPUT;
import static com.cloudera.streaming.examples.flink.Constants.K_KAFKA_TOPIC;

/**
 * Channels a Kafka topic to an HDFS file.
 */
public class KafkaToHDFSSimpleJob {

	public static void main(String[] args) throws Exception {
		ParameterTool params = Utils.parseArgs(args);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		KafkaSource<String> consumer = KafkaSource.<String>builder()
				.setBootstrapServers(params.get(K_BOOTSTRAP_SERVERS))
				.setTopics(params.get(K_KAFKA_TOPIC))
				.setValueOnlyDeserializer(new SimpleStringSchema())
				.setProperties(Utils.readKafkaProperties(params))
				.build();
		DataStream<String> source = env.fromSource(consumer, WatermarkStrategy.noWatermarks(), "Kafka Source").uid("kafka-source");

		StreamingFileSink<String> sink = StreamingFileSink
				.forRowFormat(new Path(params.getRequired(K_HDFS_OUTPUT)), new SimpleStringEncoder<String>("UTF-8"))
				.build();

		source.addSink(sink)
				.name("FS Sink")
				.uid("fs-sink");
		source.print();

		env.execute("Secured Flink Streaming Job");
	}
}
