package com.cloudera.streaming.examples.flink.types;

import java.util.Objects;

public class HeapStats {

    public static final String OLD_GEN = "PS Old Gen";
    public static final String EDEN = "PS Eden Space";
    public static final String SURVIVOR = "PS Survivor Space";

    public String area;
    /**
     * Bytes used for the old generation of the heap.
     */
    public long used;
    /**
     * Maximum bytes allocated for the old generation of the heap.
     */
    public long max;
    /**
     * Ratio of used out of the maximum old generation heap.
     */
    public double ratio;

    /**
     * ID of the Flink job
     */
    public Integer jobId;

    /**
     * Host the Flink job is running on
     */
    public String hostname;

    public HeapStats() {
    }

    public HeapStats(String area, long used, long max, double ratio, Integer jobId, String hostname) {
        this.area = area;
        this.used = used;
        this.max = max;
        this.ratio = ratio;
        this.jobId = jobId;
        this.hostname = hostname;
    }

    @Override
    public String toString() {
        return "HeapStats{" +
                "area=" + area +
                ", used=" + used +
                ", max=" + max +
                ", ratio=" + ratio +
                ", jobId=" + jobId +
                ", hostname='" + hostname + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        HeapStats heapStats = (HeapStats) o;
        return used == heapStats.used &&
                max == heapStats.max &&
                Double.compare(heapStats.ratio, ratio) == 0 &&
                Objects.equals(area, heapStats.area) &&
                Objects.equals(jobId, heapStats.jobId) &&
                Objects.equals(hostname, heapStats.hostname);
    }

    @Override
    public int hashCode() {
        return Objects.hash(area, used, max, ratio, jobId, hostname);
    }
}
