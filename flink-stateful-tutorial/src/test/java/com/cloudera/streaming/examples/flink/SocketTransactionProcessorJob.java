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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import com.cloudera.streaming.examples.flink.operators.ItemTransactionGeneratorSource;
import com.cloudera.streaming.examples.flink.types.ItemTransaction;
import com.cloudera.streaming.examples.flink.types.Query;
import com.cloudera.streaming.examples.flink.types.QueryResult;
import com.cloudera.streaming.examples.flink.types.TransactionResult;
import com.cloudera.streaming.examples.flink.types.TransactionSummary;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Simple socket based pipeline for testing the application locally. Before running start a socket connection:
 *
 * <p>nc -lk 9999
 *
 * <p>Once the job started you can send queries in the form:
 *
 * <p{"queryId":<queryId>, "itemId":"<itemId>"}
 *
 * <p>For example:
 *
 * <p>{"queryId":123, "itemId":"item_2"}
 */
public class SocketTransactionProcessorJob extends ItemTransactionJob {

	public static void main(String[] args) throws Exception {
		new SocketTransactionProcessorJob()
				.createApplicationPipeline(ParameterTool.fromArgs(new String[]{"--minimum.summary.vol", "850", "--sleep", "1"}))
				.execute();
	}

	@Override
	public void writeQueryOutput(ParameterTool params, DataStream<QueryResult> queryResultStream) {
		queryResultStream.printToErr();
	}

	@Override
	protected void writeTransactionResults(ParameterTool params, DataStream<TransactionResult> transactionresults) {
		// Ignore them for now
	}

	@Override
	protected void writeTransactionSummaries(ParameterTool params, DataStream<TransactionSummary> transactionSummaryStream) {
		// transactionSummaryStream.print();
	}

	@Override
	public DataStream<Query> readQueryStream(ParameterTool params, StreamExecutionEnvironment env) {
		return env.socketTextStream("localhost", 9999).flatMap(new FlatMapFunction<String, Query>() {
			private ObjectMapper om = new ObjectMapper();

			@Override
			public void flatMap(String s, Collector<Query> out) throws Exception {
				try {
					out.collect(om.readValue(s, Query.class));
				} catch (Throwable t) {}
			}
		});
	}

	@Override
	public DataStream<ItemTransaction> readTransactionStream(ParameterTool params, StreamExecutionEnvironment env) {
		return env.addSource(new ItemTransactionGeneratorSource(params));
	}
}
