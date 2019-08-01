package com.cloudera.streaming.examples.flink.types;

import java.util.Objects;

public class HeapAlert {

    private static String GC_EXPECTED = "Full GC expected soon";
    private static String CRITICAL = "Critical old gen usage";

    public String message;

    public HeapStats triggeringStats;

    public HeapAlert() {}

    public HeapAlert(String message, HeapStats triggeringStats) {
        this.message = message;
        this.triggeringStats = triggeringStats;
    }

    public static HeapAlert GCWarning(HeapStats stat) {
        return new HeapAlert(GC_EXPECTED, stat);
    }

    public static HeapAlert criticalOldGen(HeapStats stat) {
        return new HeapAlert(CRITICAL, stat);
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
