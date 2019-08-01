package com.cloudera.streaming.examples.flink.types;

import java.util.Objects;

public class HeapAlert {

    public String message;

    public HeapStats triggeringStats;

    public HeapAlert() {}

    public HeapAlert(String message, HeapStats triggeringStats) {
        this.message = message;
        this.triggeringStats = triggeringStats;
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
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HeapAlert heapAlert = (HeapAlert) o;
        return Objects.equals(message, heapAlert.message) &&
                Objects.equals(triggeringStats, heapAlert.triggeringStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(message, triggeringStats);
    }
}
