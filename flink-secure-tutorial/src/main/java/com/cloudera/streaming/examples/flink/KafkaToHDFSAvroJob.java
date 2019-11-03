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
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.registry.cloudera.SchemaRegistryDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;

import com.cloudera.streaming.examples.flink.data.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.cloudera.streaming.examples.flink.Constants.K_HDFS_OUTPUT;
import static com.cloudera.streaming.examples.flink.Constants.K_KAFKA_TOPIC;

public class KafkaToHDFSAvroJob {

	private static Logger LOG = LoggerFactory.getLogger(KafkaToHDFSAvroJob.class);

	public static void main(String[] args) throws Exception {

		ParameterTool params = Utils.parseArgs(args);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		KafkaDeserializationSchema<Message> schema = SchemaRegistryDeserializationSchema
				.builder(Message.class)
				.setConfig(Utils.readSchemaRegistryProperties(params))
				.build();

		FlinkKafkaConsumer<Message> consumer = new FlinkKafkaConsumer<Message>(params.getRequired(K_KAFKA_TOPIC), schema, Utils.readKafkaProperties(params));

		DataStream<String> source = env.addSource(consumer)
				.name("Kafka Source")
				.uid("Kafka Source")
				.map(record -> record.getId() + "," + record.getName() + "," + record.getDescription())
				.name("ToOutputString");

		StreamingFileSink<String> sink = StreamingFileSink
				.forRowFormat(new Path(params.getRequired(K_HDFS_OUTPUT)), new SimpleStringEncoder<String>("UTF-8"))
				.build();

		source.addSink(sink)
				.name("FS Sink")
				.uid("FS Sink");

		source.print();

		env.execute("Flink Streaming Secured Job Sample");
	}

}
