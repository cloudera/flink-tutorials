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
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.formats.registry.cloudera.avro.ClouderaRegistryAvroKafkaRecordSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import com.cloudera.streaming.examples.flink.data.Message;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.concurrent.ThreadLocalRandom;

import static com.cloudera.streaming.examples.flink.Constants.K_BOOTSTRAP_SERVERS;
import static com.cloudera.streaming.examples.flink.Constants.K_KAFKA_TOPIC;

/**
 * Generates random Messages to a Kafka topic.
 */
public class AvroDataGeneratorJob {

	public static void main(String[] args) throws Exception {
		ParameterTool params = Utils.parseArgs(args);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		String topic = params.getRequired(K_KAFKA_TOPIC);
		KafkaRecordSerializationSchema<Message> schema = ClouderaRegistryAvroKafkaRecordSerializationSchema
				.<Message>builder(topic)
				.setConfig(Utils.readSchemaRegistryProperties(params))
				.setKey(Message::getId)
				.build();

		KafkaSink<Message> kafkaSink = KafkaSink.<Message>builder()
				.setBootstrapServers(params.get(K_BOOTSTRAP_SERVERS))
				.setRecordSerializer(schema)
				.setKafkaProducerConfig(Utils.readKafkaProperties(params))
				.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
				.build();

		DataStream<Message> input = env.addSource(new DataGeneratorSource())
				.name("Data Generator Source");

		input.sinkTo(kafkaSink)
				.name("Kafka Sink")
				.uid("kafka-sink");

		input.print();

		env.execute("Avro Data Generator Job");
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
