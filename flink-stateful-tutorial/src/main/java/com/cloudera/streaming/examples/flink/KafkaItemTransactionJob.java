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
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.cloudera.streaming.examples.flink.types.ItemTransaction;
import com.cloudera.streaming.examples.flink.types.Query;
import com.cloudera.streaming.examples.flink.types.QueryResult;
import com.cloudera.streaming.examples.flink.types.QueryResultSchema;
import com.cloudera.streaming.examples.flink.types.QuerySchema;
import com.cloudera.streaming.examples.flink.types.TransactionResult;
import com.cloudera.streaming.examples.flink.types.TransactionSchema;
import com.cloudera.streaming.examples.flink.types.TransactionSummary;
import com.cloudera.streaming.examples.flink.utils.Utils;

import java.time.Duration;

/**
 * {@link ItemTransactionJob} implementation that reads and writes data using Kafka.
 */
public class KafkaItemTransactionJob extends ItemTransactionJob {

	public static final String TRANSACTION_INPUT_TOPIC_KEY = "transaction.input.topic";
	public static final String QUERY_INPUT_TOPIC_KEY = "query.input.topic";
	public static final String QUERY_OUTPUT_TOPIC_KEY = "query.output.topic";
	public static final String KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";

	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			throw new RuntimeException("Path to the properties file is expected as the only argument.");
		}
		ParameterTool params = ParameterTool.fromPropertiesFile(args[0]);
		new KafkaItemTransactionJob()
				.createApplicationPipeline(params)
				.execute("Kafka Transaction Processor Job");
	}

	public DataStream<Query> readQueryStream(ParameterTool params, StreamExecutionEnvironment env) {
		// We read queries in a simple String format and parse it to our Query object
		String topic = params.getRequired(QUERY_INPUT_TOPIC_KEY);
		KafkaSource<Query> rawQuerySource = KafkaSource.<Query>builder()
				.setBootstrapServers(params.get(KAFKA_BOOTSTRAP_SERVERS))
				.setTopics(topic)
				.setValueOnlyDeserializer(new QuerySchema(topic))
				// The first time the job is started we start from the end of the queue, ignoring earlier queries
				.setStartingOffsets(OffsetsInitializer.latest())
				.setProperties(Utils.readKafkaProperties(params))
				.build();

		return env.fromSource(rawQuerySource, WatermarkStrategy.noWatermarks(), "Kafka Query Source")
				.uid("kafka-query-source");
	}

	public DataStream<ItemTransaction> readTransactionStream(ParameterTool params, StreamExecutionEnvironment env) {
		// We read the ItemTransaction objects directly using the schema
		String topic = params.getRequired(TRANSACTION_INPUT_TOPIC_KEY);
		KafkaSource<ItemTransaction> transactionSource = KafkaSource.<ItemTransaction>builder()
				.setBootstrapServers(params.get(KAFKA_BOOTSTRAP_SERVERS))
				.setTopics(topic)
				.setValueOnlyDeserializer(new TransactionSchema(topic))
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setProperties(Utils.readKafkaProperties(params))
				.build();

		// In case event time processing is enabled we assign trailing watermarks for each partition
		return env.fromSource(transactionSource, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMinutes(1)), "Kafka Transaction Source")
				.uid("kafka-transaction-source");
	}

	public void writeQueryOutput(ParameterTool params, DataStream<QueryResult> queryResultStream) {
		// Query output is written back to kafka in a tab delimited format for readability
		String topic = params.getRequired(QUERY_OUTPUT_TOPIC_KEY);
		KafkaSink<QueryResult> queryOutputSink = KafkaSink.<QueryResult>builder()
				.setBootstrapServers(params.get(KAFKA_BOOTSTRAP_SERVERS))
				.setRecordSerializer(KafkaRecordSerializationSchema.builder()
						.setTopic(topic)
						.setValueSerializationSchema(new QueryResultSchema(topic))
						.build())
				.setKafkaProducerConfig(Utils.readKafkaProperties(params))
				.setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
				.build();

		queryResultStream.sinkTo(queryOutputSink)
				.name("Kafka Query Result Sink")
				.uid("kafka-query-result-sink");
	}

	@Override
	protected void writeTransactionResults(ParameterTool params, DataStream<TransactionResult> transactionResults) {
		// Ignore for now
	}

	@Override
	protected void writeTransactionSummaries(ParameterTool params, DataStream<TransactionSummary> transactionSummaryStream) {
		// Ignore for now
	}
}
