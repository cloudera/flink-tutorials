package com.cloudera.streaming.examples.flink.operators;

import com.cloudera.streaming.examples.flink.types.ItemTransaction;
import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.concurrent.ThreadLocalRandom;

public class ItemTransactionGeneratorSource implements ParallelSourceFunction<ItemTransaction> {

    public static final String NUM_ITEMS_KEY = "num.items";
    public static final String SLEEP_KEY = "sleep";
    public static final int DEFAULT_NUM_ITEMS = 1_000;

    private final int numItems;
    private final long sleep;
    private volatile boolean isRunning = true;

    private CircularFifoBuffer buffer;

    public ItemTransactionGeneratorSource(ParameterTool params) {
        this.numItems = params.getInt(NUM_ITEMS_KEY, DEFAULT_NUM_ITEMS);
        this.sleep = params.getLong(SLEEP_KEY, 0);
        this.buffer = new CircularFifoBuffer(Math.max(1, numItems / 100));
    }

    @Override
    public void run(SourceContext<ItemTransaction> ctx) throws Exception {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        while (isRunning) {
            String itemId;
            if (buffer.isEmpty() || rnd.nextDouble() < 0.9) {
                itemId = "item_" + rnd.nextInt(numItems);
            } else {
                itemId = (String) buffer.remove();
            }
            buffer.add(itemId);

            int quantity = (int) (Math.round(rnd.nextGaussian() / 2 * 10) * 10) + 5;
            if (quantity == 0) {
                continue;
            }
            long transactionId = rnd.nextLong(Long.MAX_VALUE);
            ctx.collect(new ItemTransaction(transactionId, System.currentTimeMillis(), itemId, quantity));
            if (sleep > 0) {
                Thread.sleep(sleep);
            }
        }

    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
