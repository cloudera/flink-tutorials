package com.cloudera.streaming.examples.flink;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;
import java.util.stream.IntStream;

public class DummySource extends RichParallelSourceFunction<String> {

    private boolean running = true;

    Random random = new Random();

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {

        Thread.sleep((int) (Math.random() * 1000));

        while (running) {
            sourceContext.collect(String.format("job_%s-%s", this.getRuntimeContext().getIndexOfThisSubtask(), System.currentTimeMillis()));
            Thread.sleep(1000);
        }

    }

    @Override
    public void cancel() {
        this.running = false;
    }

}
