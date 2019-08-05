package com.cloudera.streaming.examples.flink.types;

import java.util.Objects;

public class TransactionResult {

    public ItemTransaction transaction;

    public boolean success;

    public TransactionResult() {}

    public TransactionResult(ItemTransaction transaction, boolean success) {
        this.transaction = transaction;
        this.success = success;
    }

    @Override
    public String toString() {
        return "ItemTransactionResult{" +
                "transaction=" + transaction +
                ", success=" + success +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        TransactionResult that = (TransactionResult) o;
        return success == that.success &&
                Objects.equals(transaction, that.transaction);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transaction, success);
    }
}
