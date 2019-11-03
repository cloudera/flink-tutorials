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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.cloudera.streaming.examples.flink.types.ItemInfo;
import com.cloudera.streaming.examples.flink.types.ItemTransaction;
import com.cloudera.streaming.examples.flink.types.Query;
import com.cloudera.streaming.examples.flink.types.QueryResult;
import com.cloudera.streaming.examples.flink.types.TransactionResult;
import com.cloudera.streaming.examples.flink.types.TransactionSummary;
import com.cloudera.streaming.examples.flink.utils.testing.CollectingSink;
import com.cloudera.streaming.examples.flink.utils.testing.JobTester;
import com.cloudera.streaming.examples.flink.utils.testing.ManualSource;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TransactionProcessorTest extends ItemTransactionJob {

	private ManualSource<ItemTransaction> transactionSource;
	private ManualSource<Query> querySource;

	private CollectingSink<QueryResult> queryResultSink = new CollectingSink<>();
	private CollectingSink<TransactionResult> transactionResultSink = new CollectingSink<>();

	@Test
	public void runTest() throws Exception {
		JobTester.startTest(createApplicationPipeline(ParameterTool.fromArgs(new String[]{})));

		ItemTransaction it1 = new ItemTransaction(1, 2, "item_1", 100);
		transactionSource.sendRecord(it1);
		assertEquals(new TransactionResult(it1, true), transactionResultSink.poll());

		querySource.sendRecord(new Query(0, "item_1"));
		assertEquals(new QueryResult(0, new ItemInfo("item_1", 100)), queryResultSink.poll());

		querySource.sendRecord(new Query(3, "item_2"));
		assertEquals(new QueryResult(3, null), queryResultSink.poll());

		JobTester.stopTest();

		assertTrue(transactionResultSink.isEmpty());
		assertTrue(queryResultSink.isEmpty());
	}

	@Override
	public void writeQueryOutput(ParameterTool params, DataStream<QueryResult> queryResultStream) {
		queryResultStream.addSink(queryResultSink);
	}

	@Override
	protected void writeTransactionResults(ParameterTool params, DataStream<TransactionResult> transactionResults) {
		transactionResults.addSink(transactionResultSink);
	}

	@Override
	protected void writeTransactionSummaries(ParameterTool params, DataStream<TransactionSummary> transactionSummaryStream) {
		//ignore
	}

	@Override
	public DataStream<Query> readQueryStream(ParameterTool params, StreamExecutionEnvironment env) {
		querySource = JobTester.createManualSource(env, TypeInformation.of(Query.class));
		return querySource.getDataStream();
	}

	@Override
	public DataStream<ItemTransaction> readTransactionStream(ParameterTool params, StreamExecutionEnvironment env) {
		transactionSource = JobTester.createManualSource(env, TypeInformation.of(ItemTransaction.class));
		return transactionSource.getDataStream();
	}
}
