package com.cloudera.streaming.examples.flink.types;

import java.util.Objects;

public class ItemInfo {

    public String itemId;

    public int quantity;

    public ItemInfo() {
    }

    public ItemInfo(String itemId, int quantity) {
        this.itemId = itemId;
        this.quantity = quantity;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        ItemInfo itemInfo = (ItemInfo) o;
        return quantity == itemInfo.quantity &&
                Objects.equals(itemId, itemInfo.itemId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(itemId, quantity);
    }

    @Override
    public String toString() {
        return "ItemInfo{" +
                "itemId='" + itemId + '\'' +
                ", quantity=" + quantity +
                '}';
    }
}
