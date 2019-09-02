package com.cloudera.streaming.examples.flink.types;

import java.util.Objects;

public class QueryResult {

    public long queryId;

    public ItemInfo itemInfo;

    public QueryResult() {
    }

    public QueryResult(long queryId, ItemInfo itemInfo) {
        this.queryId = queryId;
        this.itemInfo = itemInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        QueryResult that = (QueryResult) o;
        return queryId == that.queryId &&
                Objects.equals(itemInfo, that.itemInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryId, itemInfo);
    }

    @Override
    public String toString() {
        return "QueryResult{" +
                "queryId=" + queryId +
                ", itemInfo=" + itemInfo +
                '}';
    }
}
