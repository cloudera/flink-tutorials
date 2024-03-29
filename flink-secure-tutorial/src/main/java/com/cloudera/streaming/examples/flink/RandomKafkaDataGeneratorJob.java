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

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static com.cloudera.streaming.examples.flink.Constants.K_BOOTSTRAP_SERVERS;
import static com.cloudera.streaming.examples.flink.Constants.K_KAFKA_TOPIC;

/**
 * Generates random UUID strings to a Kafka topic.
 */
public class RandomKafkaDataGeneratorJob {

	public static void main(String[] args) throws Exception {
		ParameterTool params = Utils.parseArgs(args);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
				.setBootstrapServers(params.get(K_BOOTSTRAP_SERVERS))
				.setRecordSerializer(KafkaRecordSerializationSchema.builder()
						.setTopic(params.get(K_KAFKA_TOPIC))
						.setValueSerializationSchema(new SimpleStringSchema())
						.build()
				)
				.setKafkaProducerConfig(Utils.readKafkaProperties(params))
				.build();

		DataStream<String> input = env.addSource(new UUIDGeneratorSource())
				.name("Data Generator Source")
				.uid("data-generator-source");

		input.sinkTo(kafkaSink)
				.name("Kafka Sink")
				.uid("kafka-sink");

		input.print();

		env.execute("String Data Generator Job");
	}

	/**
	 * Source generating random UUID strings.
	 */
	public static class UUIDGeneratorSource implements ParallelSourceFunction<String> {

		private volatile boolean isRunning = true;

		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			while (isRunning) {
				ctx.collect(UUID.randomUUID().toString());
				Thread.sleep(Math.abs(ThreadLocalRandom.current().nextInt()) % 1000);
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}
}
