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
		itemRead = getRuntimeContext().getMetricGroup().histogram("ItemRead", new ExponentialHistogram());
		itemWrite = getRuntimeContext().getMetricGroup().histogram("ItemWrite", new ExponentialHistogram());

		itemState = getRuntimeContext().getState(new ValueStateDescriptor<>("itemInfo", ItemInfo.class));
	}
}
