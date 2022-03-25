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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;

import com.cloudera.streaming.examples.flink.operators.ItemInfoEnrichment;
import com.cloudera.streaming.examples.flink.operators.MaxWatermarkGeneratorSupplier;
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
	public static final String TIME_INTERVALS_IN_MINUTES = "time.intervals.in.minutes";

	public static final String ENABLE_DB_ENRICHMENT = "enable.db.enrichment";
	public static final String DB_CONN_STRING = "db.connection.string";
	public static final String ASYNC_TP_SIZE = "async.threadpool.size";

	public static final OutputTag<QueryResult> QUERY_RESULT = new OutputTag<>("query-result", TypeInformation.of(QueryResult.class));

	public final StreamExecutionEnvironment createApplicationPipeline(ParameterTool params) {

		// Create the StreamExecutionEnvironment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Read transaction stream
		DataStream<ItemTransaction> transactionStream = readTransactionStream(params, env);

		// We read the query stream and exclude it from watermark tracking by assigning Watermark.MAX_WATERMARK
		DataStream<Query> queryStream = readQueryStream(params, env)
				.assignTimestampsAndWatermarks(WatermarkStrategy.forGenerator(new MaxWatermarkGeneratorSupplier<>()))
				.name("MaxWatermark")
				.uid("max-watermark");

		// Connect transactions with queries using the same itemId key and apply our transaction processor
		// The main output is the transaction result, query results are accessed as a side output.
		SingleOutputStreamOperator<TransactionResult> processedTransactions = transactionStream.keyBy(t -> t.itemId)
				.connect(queryStream.keyBy(q -> q.itemId))
				.process(new TransactionProcessor())
				.name("Transaction Processor")
				.uid("transaction-processor");

		// Query results are accessed as a side output of the transaction processor
		DataStream<QueryResult> queryResultStream = processedTransactions.getSideOutput(QUERY_RESULT);

		// If needed we enrich each query result by implementing an asynchronous enrichment operator (ItemInfoEnrichment)
		if (params.getBoolean(ENABLE_DB_ENRICHMENT, false)) {
			queryResultStream = AsyncDataStream.unorderedWait(
					queryResultStream,
					new ItemInfoEnrichment(params.getInt(ASYNC_TP_SIZE, 5), params.getRequired(DB_CONN_STRING)),
					10, TimeUnit.SECONDS)
					.name("Query Result Enrichment")
					.uid("query-result-enrichment");
		}

		// Handle the output of transaction and query results separately
		writeTransactionResults(params, processedTransactions);
		writeQueryOutput(params, queryResultStream);

		// If needed we create a window computation of the transaction summaries by item and time window
		if (params.getBoolean(ENABLE_SUMMARIES_KEY, false)) {
			DataStream<TransactionSummary> transactionSummaryStream = processedTransactions
					.keyBy(res -> res.transaction.itemId)
					.window(createTimeWindow(params))
					.aggregate(new TransactionSummaryAggregator())
					.name("Create Transaction Summary")
					.uid("create-transaction-summary")
					.filter(new SummaryAlertingCondition(params))
					.name("Filter High failure rate")
					.uid("filter-high-failure-rate");

			writeTransactionSummaries(params, transactionSummaryStream);
		}

		return env;
	}

	private WindowAssigner<Object, TimeWindow> createTimeWindow(ParameterTool params) {
		if (params.getBoolean(EVENT_TIME_KEY, false)) {
			return TumblingEventTimeWindows.of(Time.minutes(params.getInt(TIME_INTERVALS_IN_MINUTES, 10)));
		}
		return TumblingProcessingTimeWindows.of(Time.minutes(params.getInt(TIME_INTERVALS_IN_MINUTES, 10)));
	}

	protected abstract DataStream<Query> readQueryStream(ParameterTool params, StreamExecutionEnvironment env);

	protected abstract DataStream<ItemTransaction> readTransactionStream(ParameterTool params, StreamExecutionEnvironment env);

	protected abstract void writeQueryOutput(ParameterTool params, DataStream<QueryResult> queryResultStream);

	protected abstract void writeTransactionResults(ParameterTool params, DataStream<TransactionResult> transactionResults);

	protected abstract void writeTransactionSummaries(ParameterTool params, DataStream<TransactionSummary> transactionSummaryStream);

}
