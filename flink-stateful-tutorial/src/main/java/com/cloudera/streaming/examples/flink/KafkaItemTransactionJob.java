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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

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
		FlinkKafkaConsumer<Query> rawQuerySource = new FlinkKafkaConsumer<>(
				topic, new QuerySchema(topic),
				Utils.readKafkaProperties(params));

		rawQuerySource.setCommitOffsetsOnCheckpoints(true);

		// The first time the job is started we start from the end of the queue, ignoring earlier queries
		rawQuerySource.setStartFromLatest();

		return env.addSource(rawQuerySource)
				.name("Kafka Query Source")
				.uid("Kafka Query Source");
	}

	public DataStream<ItemTransaction> readTransactionStream(ParameterTool params, StreamExecutionEnvironment env) {
		// We read the ItemTransaction objects directly using the schema
		String topic = params.getRequired(TRANSACTION_INPUT_TOPIC_KEY);
		FlinkKafkaConsumer<ItemTransaction> transactionSource = new FlinkKafkaConsumer<>(
				topic, new TransactionSchema(topic),
				Utils.readKafkaProperties(params));

		transactionSource.setCommitOffsetsOnCheckpoints(true);
		transactionSource.setStartFromEarliest();

		// In case event time processing is enabled we assign trailing watermarks for each partition
		transactionSource.assignTimestampsAndWatermarks(
				WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMinutes(1)));

		return env.addSource(transactionSource)
				.name("Kafka Transaction Source")
				.uid("Kafka Transaction Source");
	}

	public void writeQueryOutput(ParameterTool params, DataStream<QueryResult> queryResultStream) {
		// Query output is written back to kafka in a tab delimited format for readability
		String topic = params.getRequired(QUERY_OUTPUT_TOPIC_KEY);
		FlinkKafkaProducer<QueryResult> queryOutputSink = new FlinkKafkaProducer<>(
				topic, new QueryResultSchema(topic),
				Utils.readKafkaProperties(params),
				FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);

		queryResultStream
				.addSink(queryOutputSink)
				.name("Kafka Query Result Sink")
				.uid("Kafka Query Result Sink");
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
