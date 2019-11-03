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

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.formats.avro.registry.cloudera.SchemaRegistrySerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;

import com.cloudera.streaming.examples.flink.data.Message;
import org.apache.commons.lang.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static com.cloudera.streaming.examples.flink.Constants.K_KAFKA_TOPIC;

public class AvroDataGeneratorJob {

	private static Logger LOG = LoggerFactory.getLogger(AvroDataGeneratorJob.class);

	public static void main(String[] args) throws Exception {
		ParameterTool params = Utils.parseArgs(args);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		KafkaSerializationSchema<Message> schema = SchemaRegistrySerializationSchema.<Message>
				builder(params.getRequired(K_KAFKA_TOPIC))
				.setConfig(Utils.readSchemaRegistryProperties(params))
				.setKey(Message::getId)
				.build();

		FlinkKafkaProducer<Message> kafkaSink = new FlinkKafkaProducer<>(
				"default", schema, Utils.readKafkaProperties(params), FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);

		DataStream<Message> input = env.addSource(new DataGeneratorSource()).name("Data Generator Source");

		input.addSink(kafkaSink)
				.name("Kafka Sink")
				.uid("Kafka Sink");

		input.print();

		env.execute("Data Generator Job");
	}

	public static class DataGeneratorSource implements ParallelSourceFunction<Message> {

		private volatile boolean isRunning = true;

		@Override
		public void run(SourceContext<Message> ctx) throws Exception {
			ThreadLocalRandom rnd = ThreadLocalRandom.current();
			while (isRunning) {
				synchronized (ctx.getCheckpointLock()) {
					ctx.collect(new Message(UUID.randomUUID().toString() + " - " + LocalDateTime.now().toString(),
							RandomStringUtils.randomAlphabetic(10),
							RandomStringUtils.randomAlphabetic(100)));
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
