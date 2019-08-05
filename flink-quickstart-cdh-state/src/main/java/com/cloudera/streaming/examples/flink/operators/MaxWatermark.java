package com.cloudera.streaming.examples.flink.operators;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

public final class MaxWatermark<T> implements AssignerWithPeriodicWatermarks<T> {
    private static final long serialVersionUID = 1L;

    @Override
    public long extractTimestamp(T input, long ts) {
        return Long.MAX_VALUE;
    }

    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(Long.MAX_VALUE);
    }
}
