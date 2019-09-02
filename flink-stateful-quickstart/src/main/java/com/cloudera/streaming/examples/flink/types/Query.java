package com.cloudera.streaming.examples.flink.types;

import java.util.Objects;

public class Query {

    public long queryId;

    public String itemId;

    public Query() {}

    public Query(long queryId, String itemId) {
        this.queryId = queryId;
        this.itemId = itemId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        Query itemQuery = (Query) o;
        return queryId == itemQuery.queryId &&
                Objects.equals(itemId, itemQuery.itemId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryId, itemId);
    }
}
