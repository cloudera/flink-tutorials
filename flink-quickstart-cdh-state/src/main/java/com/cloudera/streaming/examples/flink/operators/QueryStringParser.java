package com.cloudera.streaming.examples.flink.operators;

import com.cloudera.streaming.examples.flink.types.Query;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryStringParser implements FlatMapFunction<String, Query> {

    private static final Logger LOG = LoggerFactory.getLogger(QueryStringParser.class);

    @Override
    public void flatMap(String queryString, Collector<Query> out) throws Exception {
        try {
            String[] split = queryString.split(" ");
            out.collect(new Query(Long.parseLong(split[0]), split[1]));
        } catch (Exception parseErr) {
            LOG.error("Could not parse item query: {}", queryString);
        }
    }
}
