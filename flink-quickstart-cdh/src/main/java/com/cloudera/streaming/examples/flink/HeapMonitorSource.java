package com.cloudera.streaming.examples.flink;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;

public class HeapMonitorSource extends RichParallelSourceFunction<HeapStats> {
    private boolean running = true;
    private static final int MEGA = 1024*1024;
    private final long sleepMillis;

    public HeapMonitorSource(long sleepMillis){
        this.sleepMillis = sleepMillis;
    }

    @Override
    public void run(SourceFunction.SourceContext<HeapStats> sourceContext) throws Exception {
        ArrayList<Long> leak;
        ArrayList<ArrayList<Long>> bucket = new ArrayList<>();

        while (running) {
            Thread.sleep(sleepMillis);

            leak = new ArrayList<>(MEGA);
            bucket.add(leak);

            for (MemoryPoolMXBean mpBean: ManagementFactory.getMemoryPoolMXBeans()) {
                if (mpBean.getType() == MemoryType.HEAP && mpBean.getName().equals("PS Old Gen")) {
                    MemoryUsage memoryUsage = mpBean.getUsage();
                    long used = memoryUsage.getUsed();
                    long max = memoryUsage.getMax();

                    sourceContext.collect(new HeapStats(used, max, (double) used/max));
                }
            }
        }

    }

    @Override
    public void cancel() {
        this.running = false;
    }
}
