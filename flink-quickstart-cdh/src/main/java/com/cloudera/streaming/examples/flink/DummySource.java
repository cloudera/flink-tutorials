package com.cloudera.streaming.examples.flink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.net.InetAddress;
import java.util.Random;
import java.util.stream.IntStream;

public class DummySource extends RichParallelSourceFunction<Tuple4<String, Integer, String, Long>> {

    private boolean running = true;

    private RuntimeContext ctx;

    private String hostname;

    @Override
    public void run(SourceContext<Tuple4<String, Integer, String, Long>> sourceContext) throws Exception {

        Thread.sleep((int) (Math.random() * 1000));

        ctx = this.getRuntimeContext();
        hostname = InetAddress.getLocalHost().getHostName();

        while (running) {
            Tuple4<String, Integer, String, Long> record = new Tuple4<>(ctx.getTaskName(), ctx.getIndexOfThisSubtask(), hostname, System.currentTimeMillis());
            sourceContext.collect(record);
            Thread.sleep((ctx.getIndexOfThisSubtask() + 1) * 1000);
        }

    }

    @Override
    public void cancel() {
        this.running = false;
    }

}
