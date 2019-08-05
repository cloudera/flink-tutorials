package com.cloudera.streaming.examples.flink.types;

import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

import java.nio.charset.StandardCharsets;

public class QueryResultSchema implements KeyedSerializationSchema<QueryResult> {

    @Override
    public byte[] serializeKey(QueryResult res) {
        return String.valueOf(res.queryId).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] serializeValue(QueryResult res) {
        return (res.queryId + "\t" + res.itemInfo.itemId + "\t" + res.itemInfo.quantity).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String getTargetTopic(QueryResult res) {
        return null;
    }

}
