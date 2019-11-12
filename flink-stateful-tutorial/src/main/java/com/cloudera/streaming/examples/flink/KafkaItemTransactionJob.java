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

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import com.cloudera.streaming.examples.flink.operators.HashingKafkaPartitioner;
import com.cloudera.streaming.examples.flink.operators.QueryStringParser;
import com.cloudera.streaming.examples.flink.types.ItemTransaction;
import com.cloudera.streaming.examples.flink.types.Query;
import com.cloudera.streaming.examples.flink.types.QueryResult;
import com.cloudera.streaming.examples.flink.types.QueryResultSchema;
import com.cloudera.streaming.examples.flink.types.TransactionResult;
import com.cloudera.streaming.examples.flink.types.TransactionSchema;
import com.cloudera.streaming.examples.flink.types.TransactionSummary;
import com.cloudera.streaming.examples.flink.utils.Utils;

import java.util.Optional;

/**
 * {@link ItemTransactionJob} implementation that reads and writes data using Kafka.
 */
public class KafkaItemTransactionJob extends ItemTransactionJob {

	public static String TRANSACTION_INPUT_TOPIC_KEY = "transaction.input.topic";
	public static String QUERY_INPUT_TOPIC_KEY = "query.input.topic";
	public static String QUERY_OUTPUT_TOPIC_KEY = "query.output.topic";

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
		FlinkKafkaConsumer<String> rawQuerySource = new FlinkKafkaConsumer<>(
				params.getRequired(QUERY_INPUT_TOPIC_KEY), new SimpleStringSchema(),
				Utils.readKafkaProperties(params, true));

		rawQuerySource.setCommitOffsetsOnCheckpoints(true);

		// The first time the job is started we start from the end of the queue, ignoring earlier queries
		rawQuerySource.setStartFromLatest();

		return env.addSource(rawQuerySource)
				.name("Kafka Query Source")
				.uid("Kafka Query Source")
				.flatMap(new QueryStringParser()).name("Query parser");
	}

	public DataStream<ItemTransaction> readTransactionStream(ParameterTool params, StreamExecutionEnvironment env) {
		// We read the ItemTransaction objects directly using the schema
		FlinkKafkaConsumer<ItemTransaction> transactionSource = new FlinkKafkaConsumer<>(
				params.getRequired(TRANSACTION_INPUT_TOPIC_KEY), new TransactionSchema(),
				Utils.readKafkaProperties(params, true));

		transactionSource.setCommitOffsetsOnCheckpoints(true);
		transactionSource.setStartFromEarliest();

		// In case event time processing is enabled we assign trailing watermarks for each partition
		transactionSource.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ItemTransaction>(Time.minutes(1)) {
			@Override
			public long extractTimestamp(ItemTransaction transaction) {
				return transaction.ts;
			}
		});

		return env.addSource(transactionSource)
				.name("Kafka Transaction Source")
				.uid("Kafka Transaction Source");
	}

	public void writeQueryOutput(ParameterTool params, DataStream<QueryResult> queryResultStream) {
		// Query output is written back to kafka in a tab delimited format for readability
		FlinkKafkaProducer<QueryResult> queryOutputSink = new FlinkKafkaProducer<>(
				params.getRequired(QUERY_OUTPUT_TOPIC_KEY), new QueryResultSchema(),
				Utils.readKafkaProperties(params, false),
				Optional.of(new HashingKafkaPartitioner<>()));

		queryResultStream
				.addSink(queryOutputSink)
				.name("Kafka Query Result Sink")
				.uid("Kafka Query Result Sink");
	}

	@Override
	protected void writeTransactionResults(ParameterTool params, DataStream<TransactionResult> transactionresults) {
		// Ignore for now
	}

	@Override
	protected void writeTransactionSummaries(ParameterTool params, DataStream<TransactionSummary> transactionSummaryStream) {
		// Ignore for now
	}
}
