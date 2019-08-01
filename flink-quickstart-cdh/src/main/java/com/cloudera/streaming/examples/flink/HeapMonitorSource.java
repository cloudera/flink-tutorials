package com.cloudera.streaming.examples.flink;

import com.cloudera.streaming.examples.flink.types.HeapStats;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.net.InetAddress;
import java.util.ArrayList;

public class HeapMonitorSource extends RichParallelSourceFunction<HeapStats> {
    private boolean running = true;
    private static final int MEGA = 1024 * 1024;
    private final long sleepMillis;

    private RuntimeContext ctx;

    private String hostname;

    public HeapMonitorSource(long sleepMillis) {
        this.sleepMillis = sleepMillis;
    }

    @Override
    public void run(SourceFunction.SourceContext<HeapStats> sourceContext) throws Exception {

        ctx = this.getRuntimeContext();

        hostname = InetAddress.getLocalHost().getHostName();

        ArrayList<ArrayList<Long>> leak = new ArrayList<>();

        while (running) {
            Thread.sleep(sleepMillis);
            ArrayList<Long> largeArrayList = new ArrayList<>(MEGA);
//            leak.add(noleak);

            for (MemoryPoolMXBean mpBean : ManagementFactory.getMemoryPoolMXBeans()) {
                if (mpBean.getType() == MemoryType.HEAP) {
                    MemoryUsage memoryUsage = mpBean.getUsage();
                    long used = memoryUsage.getUsed();
                    long max = memoryUsage.getMax();

                    sourceContext.collect(new HeapStats(mpBean.getName(), used, max, (double) used / max, ctx.getIndexOfThisSubtask(), hostname));
                }
            }
        }

    }

    @Override
    public void cancel() {
        this.running = false;
    }


}
