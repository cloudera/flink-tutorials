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

package com.cloudera.streaming.examples.flink.operators;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import com.cloudera.streaming.examples.flink.KafkaItemTransactionJob;
import com.cloudera.streaming.examples.flink.types.ItemInfo;
import com.cloudera.streaming.examples.flink.types.ItemTransaction;
import com.cloudera.streaming.examples.flink.types.Query;
import com.cloudera.streaming.examples.flink.types.QueryResult;
import com.cloudera.streaming.examples.flink.types.TransactionResult;
import com.cloudera.streaming.examples.flink.utils.ExponentialHistogram;

/**
 * Core transaction and query processing logic. {@link #processElement1(ItemTransaction, Context, Collector)} receives
 * transactions and executes them if there is sufficient quantity already stored in the state.
 * <p>
 * {@link #processElement2(Query, Context, Collector)} receives item queries that just returns the current info for the
 * queried item.
 * <p>
 * Both processing functions are keyed by the itemId field.
 * <p>
 * In addition to the core logic we added custom histogram metrics to track state access time for future optimizations.
 */
public class TransactionProcessor extends KeyedCoProcessFunction<String, ItemTransaction, Query, TransactionResult> {

	private transient ValueState<ItemInfo> itemState;
	private transient Histogram itemRead;
	private transient Histogram itemWrite;

	@Override
	public void processElement1(ItemTransaction transaction, Context ctx, Collector<TransactionResult> out) throws Exception {
		long startTime = System.nanoTime();
		ItemInfo info = itemState.value();
		itemRead.update(System.nanoTime() - startTime);

		if (info == null) {
			info = new ItemInfo(transaction.itemId, 0);
		}
		int newQuantity = info.quantity + transaction.quantity;

		boolean success = newQuantity >= 0;
		if (success) {
			info.quantity = newQuantity;
			startTime = System.nanoTime();
			itemState.update(info);
			itemWrite.update(System.nanoTime() - startTime);
		}
		out.collect(new TransactionResult(transaction, success));
	}

	@Override
	public void processElement2(Query query, Context ctx, Collector<TransactionResult> out) throws Exception {
		ctx.output(KafkaItemTransactionJob.QUERY_RESULT, new QueryResult(query.queryId, itemState.value()));
	}

	@Override
	public void open(Configuration parameters) {
		// We create state read/write time metrics for later performance tuning
		itemRead = getRuntimeContext().getMetricGroup().histogram("ItemRead", new ExponentialHistogram());
		itemWrite = getRuntimeContext().getMetricGroup().histogram("ItemWrite", new ExponentialHistogram());

		itemState = getRuntimeContext().getState(new ValueStateDescriptor<>("itemInfo", ItemInfo.class));
	}
}
