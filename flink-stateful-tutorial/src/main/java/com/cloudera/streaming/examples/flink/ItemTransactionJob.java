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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import com.cloudera.streaming.examples.flink.operators.MaxWatermark;
import com.cloudera.streaming.examples.flink.operators.SummaryAlertingCondition;
import com.cloudera.streaming.examples.flink.operators.TransactionProcessor;
import com.cloudera.streaming.examples.flink.operators.TransactionSummaryAggregator;
import com.cloudera.streaming.examples.flink.types.ItemTransaction;
import com.cloudera.streaming.examples.flink.types.Query;
import com.cloudera.streaming.examples.flink.types.QueryResult;
import com.cloudera.streaming.examples.flink.types.TransactionResult;
import com.cloudera.streaming.examples.flink.types.TransactionSummary;

import java.util.concurrent.TimeUnit;

/**
 * Base class for out item transaction and query processor pipeline. The core processing functionality is encapsulated here while
 * subclasses have to implement input and output methods. Check the {@link KafkaItemTransactionJob} for a Kafka input/output based
 * implementation of the pipeline.
 */
public abstract class ItemTransactionJob {

	public static final String EVENT_TIME_KEY = "event.time";
	public static final String ENABLE_SUMMARIES_KEY = "enable.summaries";

	public static OutputTag<QueryResult> QUERY_RESULT = new OutputTag<QueryResult>("query-result", TypeInformation.of(QueryResult.class));

	public final StreamExecutionEnvironment createApplicationPipeline(ParameterTool params) throws Exception {

		// Create and configure the StreamExecutionEnvironment
		StreamExecutionEnvironment env = createExecutionEnvironment(params);

		// Read transaction stream
		DataStream<ItemTransaction> transactionStream = readTransactionStream(params, env);

		// We read the query stream and exclude it from watermark tracking by assigning Long.MAX_VALUE watermark
		DataStream<Query> queryStream = readQueryStream(params, env)
				.assignTimestampsAndWatermarks(new MaxWatermark<>())
				.name("MaxWatermark");

		// Connect transactions with queries using the same itemId key and apply our transaction processor
		// The main output is the transaction result, query results are accessed as a side output.
		SingleOutputStreamOperator<TransactionResult> processedTransactions = transactionStream.keyBy("itemId")
				.connect(queryStream.keyBy("itemId"))
				.process(new TransactionProcessor())
				.name("Transaction Processor")
				.uid("Transaction Processor");

		// Query results are accessed as a sideoutput of the transaction processor
		DataStream<QueryResult> queryResultStream = processedTransactions.getSideOutput(QUERY_RESULT);

		// Handle the output of transaction and query results separately
		writeTransactionResults(params, processedTransactions);
		writeQueryOutput(params, queryResultStream);

		// If needed we create a window computation of the transaction summaries by item and time window
		if (params.getBoolean(ENABLE_SUMMARIES_KEY, false)) {
			DataStream<TransactionSummary> transactionSummaryStream = processedTransactions
					.keyBy("transaction.itemId")
					.timeWindow(Time.minutes(10))
					.aggregate(new TransactionSummaryAggregator())
					.name("Create Transaction Summary")
					.uid("Create Transaction Summary")
					.filter(new SummaryAlertingCondition(params))
					.name("Filter High failure rate");

			writeTransactionSummaries(params, transactionSummaryStream);
		}

		return env;
	}

	private StreamExecutionEnvironment createExecutionEnvironment(ParameterTool params) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// We set max parallelism to a number with a lot of divisors
		env.setMaxParallelism(360);

		// Configure checkpointing if interval is set
		long cpInterval = params.getLong("checkpoint.interval.millis", TimeUnit.MINUTES.toMillis(1));
		if (cpInterval > 0) {
			CheckpointConfig checkpointConf = env.getCheckpointConfig();
			checkpointConf.setCheckpointInterval(cpInterval);
			checkpointConf.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
			checkpointConf.setCheckpointTimeout(TimeUnit.HOURS.toMillis(1));
			checkpointConf.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
			env.getConfig().setUseSnapshotCompression(true);
		}

		if (params.getBoolean(EVENT_TIME_KEY, false)) {
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		}

		return env;
	}

	protected abstract DataStream<Query> readQueryStream(ParameterTool params, StreamExecutionEnvironment env);

	protected abstract DataStream<ItemTransaction> readTransactionStream(ParameterTool params, StreamExecutionEnvironment env);

	protected abstract void writeQueryOutput(ParameterTool params, DataStream<QueryResult> queryResultStream);

	protected abstract void writeTransactionResults(ParameterTool params, DataStream<TransactionResult> transactionResults);

	protected abstract void writeTransactionSummaries(ParameterTool params, DataStream<TransactionSummary> transactionSummaryStream);

}
