package com.cloudera.streaming.examples.flink.operators;

import com.cloudera.streaming.examples.flink.KafkaItemTransactionJob;
import com.cloudera.streaming.examples.flink.types.*;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class TransactionProcessor extends CoProcessFunction<ItemTransaction, Query, TransactionResult> {

    private transient ValueState<ItemInfo> itemState;

    @Override
    public void processElement1(ItemTransaction transaction, Context ctx, Collector<TransactionResult> out) throws Exception {
        ItemInfo info = itemState.value();
        if (info == null) {
            info = new ItemInfo(transaction.itemId, 0);
        }
        int newQuantity = info.quantity + transaction.quantity;

        boolean success = newQuantity >= 0;
        if (success) {
            info.quantity = newQuantity;
            itemState.update(info);
        }
        out.collect(new TransactionResult(transaction, success));
    }

    @Override
    public void processElement2(Query query, Context ctx, Collector<TransactionResult> out) throws Exception {
        ctx.output(KafkaItemTransactionJob.QUERY_RESULT, new QueryResult(query.queryId, itemState.value()));
    }

    @Override
    public void open(Configuration parameters) {
        itemState = getRuntimeContext().getState(new ValueStateDescriptor<>("itemInfo", ItemInfo.class));
    }
}
