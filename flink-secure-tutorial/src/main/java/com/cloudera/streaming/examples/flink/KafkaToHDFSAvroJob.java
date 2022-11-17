/*
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.registry.cloudera.avro.ClouderaRegistryAvroKafkaDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;

import com.cloudera.streaming.examples.flink.data.Message;

import static com.cloudera.streaming.examples.flink.Constants.K_BOOTSTRAP_SERVERS;
import static com.cloudera.streaming.examples.flink.Constants.K_HDFS_OUTPUT;
import static com.cloudera.streaming.examples.flink.Constants.K_KAFKA_TOPIC;

/**
 * Channels a Kafka topic to an HDFS after converting it to a string.
 */
public class KafkaToHDFSAvroJob {

	public static void main(String[] args) throws Exception {
		ParameterTool params = Utils.parseArgs(args);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		KafkaDeserializationSchema<Message> schema = ClouderaRegistryAvroKafkaDeserializationSchema
				.builder(Message.class)
				.setConfig(Utils.readSchemaRegistryProperties(params))
				.build();

		KafkaSource<Message> kafkaSource = KafkaSource.<Message>builder()
				.setBootstrapServers(params.get(K_BOOTSTRAP_SERVERS))
				.setTopics(params.get(K_KAFKA_TOPIC))
				.setDeserializer(KafkaRecordDeserializationSchema.of(schema))
				.setProperties(Utils.readKafkaProperties(params))
				.build();

		DataStream<String> source = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
				.uid("kafka-source")
				.map(record -> record.getId() + "," + record.getName() + "," + record.getDescription())
				.name("To Output String")
				.uid("to-output-string");

		StreamingFileSink<String> sink = StreamingFileSink
				.forRowFormat(new Path(params.getRequired(K_HDFS_OUTPUT)), new SimpleStringEncoder<String>("UTF-8"))
				.build();

		source.addSink(sink)
				.name("FS Sink")
				.uid("fs-sink");

		source.print();

		env.execute("Secured Avro Flink Streaming Job");
	}

}
