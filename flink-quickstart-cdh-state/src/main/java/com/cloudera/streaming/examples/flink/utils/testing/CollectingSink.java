package com.cloudera.streaming.examples.flink.utils.testing;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class CollectingSink<T> extends RichSinkFunction<T> {

    private static final long serialVersionUID = 1L;
    private static final List<BlockingQueue<Object>> queues = Collections.synchronizedList(new ArrayList<>());
    private static final AtomicInteger numSinks = new AtomicInteger(-1);
    private final int index;

    public CollectingSink() {
        index = numSinks.incrementAndGet();
        queues.add(new LinkedBlockingQueue<>());
    }

    @Override
    public void invoke(T event, SinkFunction.Context context) throws Exception {
        queues.get(index).add(event);
    }

    public boolean isEmpty() {
        return queues.get(index).isEmpty();
    }

    public T poll() throws TimeoutException {
        return poll(Duration.ofSeconds(15));
    }

    public T poll(Duration duration) throws TimeoutException {
        T e = null;
        try {
            e = (T) queues.get(index).poll(duration.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }

        if (e == null) {
            throw new TimeoutException();
        } else {
            return e;
        }
    }
}
