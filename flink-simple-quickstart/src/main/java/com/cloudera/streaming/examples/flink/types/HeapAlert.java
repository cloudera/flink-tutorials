package com.cloudera.streaming.examples.flink.types;

import java.util.Objects;

public class HeapAlert {

    private static final String MASK_RATIO_MATCH_MSG = " was found in the HeapMetrics ratio.";

    public String message;
    public HeapMetrics triggeringMetrics;

    public HeapAlert() {}

    public HeapAlert(String message, HeapMetrics triggeringMetrics) {
        this.message = message;
        this.triggeringMetrics = triggeringMetrics;
    }

    public static HeapAlert maskRatioMatch(String alertMask, HeapMetrics heapMetrics){
        return new HeapAlert(alertMask + MASK_RATIO_MATCH_MSG, heapMetrics);
    }

    @Override
    public String toString() {
        return "HeapAlert{" +
                "message='" + message + '\'' +
                ", triggeringStats=" + triggeringMetrics +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        HeapAlert heapAlert = (HeapAlert) o;
        return Objects.equals(message, heapAlert.message) &&
                Objects.equals(triggeringMetrics, heapAlert.triggeringMetrics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(message, triggeringMetrics);
    }
}
