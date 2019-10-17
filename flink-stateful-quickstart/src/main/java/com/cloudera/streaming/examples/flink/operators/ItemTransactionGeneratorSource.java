/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.streaming.examples.flink.operators;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import com.cloudera.streaming.examples.flink.types.ItemTransaction;
import org.apache.commons.math3.distribution.ParetoDistribution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;

/**
 * A simple data generator that generates {@link ItemTransaction} data with a skewed itemId distribution
 * to better simulate real world access patterns with hot items.
 */
public class ItemTransactionGeneratorSource implements ParallelSourceFunction<ItemTransaction> {

	public static final String NUM_ITEMS_KEY = "num.items";
	public static final String SLEEP_KEY = "sleep";
	public static final String PARETO_SHAPE_KEY = "pareto.shape";
	public static final int DEFAULT_NUM_ITEMS = 1_000;
	public static final int DEFAULT_SHAPE = 15;
	private static final Logger LOG = LoggerFactory.getLogger(ItemTransactionGeneratorSource.class);
	private final int numItems;
	private final long sleep;
	private final int shape;
	private volatile boolean isRunning = true;

	public ItemTransactionGeneratorSource(ParameterTool params) {
		this.numItems = params.getInt(NUM_ITEMS_KEY, DEFAULT_NUM_ITEMS);
		this.sleep = params.getLong(SLEEP_KEY, 0);
		this.shape = params.getInt(PARETO_SHAPE_KEY, DEFAULT_SHAPE);
	}

	@Override
	public void run(SourceContext<ItemTransaction> ctx) throws Exception {
		ThreadLocalRandom rnd = ThreadLocalRandom.current();
		ParetoDistribution paretoDistribution = new ParetoDistribution(numItems, shape);

		LOG.info("Starting data generator for {} items and {} sleep", numItems, sleep);

		while (isRunning) {
			long nextItemId;
			do {
				nextItemId = sample(paretoDistribution);
			} while (nextItemId > numItems);
			String itemId = "item_" + nextItemId;

			int quantity = (int) (Math.round(rnd.nextGaussian() / 2 * 10) * 10) + 5;
			if (quantity == 0) {
				continue;
			}
			long transactionId = rnd.nextLong(Long.MAX_VALUE);
			synchronized (ctx.getCheckpointLock()) {
				ctx.collect(new ItemTransaction(transactionId, System.currentTimeMillis(), itemId, quantity));
			}
			if (sleep > 0) {
				Thread.sleep(sleep);
			}
		}

	}

	private long sample(ParetoDistribution paretoDistribution) {
		return (Math.round(paretoDistribution.sample() - paretoDistribution.getScale()) + 1);
	}

	@Override
	public void cancel() {
		isRunning = false;
	}
}
