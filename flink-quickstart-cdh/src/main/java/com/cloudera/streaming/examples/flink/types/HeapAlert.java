package com.cloudera.streaming.examples.flink.types;

import java.util.Objects;

public class HeapAlert {

    private static final String MASK_RATIO_MATCH_MSG = " was found in the HeapStats ratio.";

    public String message;
    public HeapStats triggeringStats;

    public HeapAlert() {}

    public HeapAlert(String message, HeapStats triggeringStats) {
        this.message = message;
        this.triggeringStats = triggeringStats;
    }

    public static HeapAlert maskRatioMatch(String alertMask, HeapStats heapStats){
        return new HeapAlert(alertMask + MASK_RATIO_MATCH_MSG, heapStats);
    }

    @Override
    public String toString() {
        return "HeapAlert{" +
                "message='" + message + '\'' +
                ", triggeringStats=" + triggeringStats +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        HeapAlert heapAlert = (HeapAlert) o;
        return Objects.equals(message, heapAlert.message) &&
                Objects.equals(triggeringStats, heapAlert.triggeringStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(message, triggeringStats);
    }
}
