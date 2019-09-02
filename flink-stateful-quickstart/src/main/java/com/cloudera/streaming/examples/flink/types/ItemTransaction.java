package com.cloudera.streaming.examples.flink.types;

import java.util.Objects;

public class ItemTransaction {

    public long transactionId;

    public long ts;

    public String itemId;

    public int quantity;

    public ItemTransaction() {}

    public ItemTransaction(long transactionId, long ts, String itemId, int quantity) {
        this.transactionId = transactionId;
        this.ts = ts;
        this.itemId = itemId;
        this.quantity = quantity;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        ItemTransaction that = (ItemTransaction) o;
        return transactionId == that.transactionId &&
                ts == that.ts &&
                quantity == that.quantity &&
                Objects.equals(itemId, that.itemId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transactionId, ts, itemId, quantity);
    }

    @Override
    public String toString() {
        return "ItemTransaction{" +
                "transactionId=" + transactionId +
                ", ts=" + ts +
                ", itemId='" + itemId + '\'' +
                ", quantity=" + quantity +
                '}';
    }
}
