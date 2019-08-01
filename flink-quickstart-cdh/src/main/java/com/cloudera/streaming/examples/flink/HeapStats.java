package com.cloudera.streaming.examples.flink;

public class HeapStats {


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
}
