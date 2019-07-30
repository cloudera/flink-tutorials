package com.cloudera.streaming.examples.flink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.net.InetAddress;
import java.util.Random;
import java.util.stream.IntStream;

public class DummySource extends RichParallelSourceFunction<DummyRecord> {

    private volatile boolean running = true;

    private RuntimeContext ctx;

    private String hostname;

    @Override
    public void run(SourceContext<DummyRecord> sourceContext) throws Exception {

        ctx = this.getRuntimeContext();
        hostname = InetAddress.getLocalHost().getHostName();

        while (running) {
            DummyRecord record = new DummyRecord(ctx.getTaskName(), ctx.getIndexOfThisSubtask(), hostname, System.currentTimeMillis());
            sourceContext.collect(record);
            Thread.sleep((ctx.getIndexOfThisSubtask() + 1) * 100);
        }

    }

    @Override
    public void cancel() {
        this.running = false;
    }

}
