package com.cloudera.streaming.examples.flink;

public class HeapStats {
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

    public HeapStats(){}

    public HeapStats(long used, long max, double ratio){
        this.used = used;
        this.max = max;
        this.ratio = ratio;
    }

    @Override
    public String toString() {
        return "HeapStats{" +
                "used=" + used +
                ", max=" + max +
                ", ratio=" + ratio +
                '}';
    }
}
