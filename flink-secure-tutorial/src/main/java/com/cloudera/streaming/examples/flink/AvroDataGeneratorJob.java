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

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.formats.avro.generated.Message;
import org.apache.flink.formats.avro.registry.cloudera.ClouderaRegistryKafkaSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;

import org.apache.commons.lang3.RandomStringUtils;

import java.util.concurrent.ThreadLocalRandom;

import static com.cloudera.streaming.examples.flink.Constants.K_KAFKA_TOPIC;

/**
 * Generates random Messages to a kafka topic.
 */
public class AvroDataGeneratorJob {

	public static void main(String[] args) throws Exception {
		ParameterTool params = Utils.parseArgs(args);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		KafkaSerializationSchema<Message> schema = ClouderaRegistryKafkaSerializationSchema.<Message>
				builder(params.getRequired(K_KAFKA_TOPIC))
				.setConfig(Utils.readSchemaRegistryProperties(params))
				.setKey(Message::getId)
				.build();

		FlinkKafkaProducer<Message> kafkaSink = new FlinkKafkaProducer<>(
				"default", schema, Utils.readKafkaProperties(params), FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);

		DataStream<Message> input = env.addSource(new DataGeneratorSource())
				.name("Data Generator Source");

		input.addSink(kafkaSink)
				.name("Kafka Sink")
				.uid("Kafka Sink");

		input.print();

		env.execute("Data Generator Job");
	}

	/**
	 * Generates Message objects with random content at random interval.
	 */
	public static class DataGeneratorSource implements ParallelSourceFunction<Message> {

		private volatile boolean isRunning = true;

		@Override
		public void run(SourceContext<Message> ctx) throws Exception {
			ThreadLocalRandom rnd = ThreadLocalRandom.current();
			while (isRunning) {
				synchronized (ctx.getCheckpointLock()) {
					ctx.collect(new Message(System.currentTimeMillis(),
							RandomStringUtils.randomAlphabetic(10),
							RandomStringUtils.randomAlphanumeric(20)));
				}

				Thread.sleep(Math.abs(rnd.nextInt()) % 1000);
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}
}
