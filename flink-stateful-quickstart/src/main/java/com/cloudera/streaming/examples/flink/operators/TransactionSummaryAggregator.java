package com.cloudera.streaming.examples.flink.operators;

import com.cloudera.streaming.examples.flink.types.TransactionResult;
import com.cloudera.streaming.examples.flink.types.TransactionSummary;
import org.apache.flink.api.common.functions.AggregateFunction;

public class TransactionSummaryAggregator implements AggregateFunction<TransactionResult, TransactionSummary, TransactionSummary> {

    @Override
    public TransactionSummary createAccumulator() {return new TransactionSummary();}

    @Override
    public TransactionSummary add(TransactionResult tr, TransactionSummary acc) {
        acc.itemId = tr.transaction.itemId;
        if (tr.success) {
            acc.numSuccessfulTransactions++;
        } else {
            acc.numFailedTransactions++;
        }
        acc.totalVolume += Math.abs(tr.transaction.quantity);
        return acc;
    }

    @Override
    public TransactionSummary getResult(TransactionSummary acc) {return acc;}

    @Override
    public TransactionSummary merge(TransactionSummary ts1, TransactionSummary ts2) {
        return new TransactionSummary(ts2.itemId != null ? ts2.itemId : ts1.itemId,
                ts1.numFailedTransactions + ts2.numFailedTransactions,
                ts1.numSuccessfulTransactions + ts2.numSuccessfulTransactions,
                ts1.totalVolume + ts2.totalVolume);
    }
}
