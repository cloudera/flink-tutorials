/*
 * Licensed to Cloudera, Inc. under one
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
import com.cloudera.streaming.examples.flink.types.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;

/**
 * A simple data generator that generates {@link ItemTransaction} data with a skewed itemId distribution
 * to better simulate real world access patterns with hot items.
 */
public class QueryGeneratorSource implements ParallelSourceFunction<Query> {

	public static final String NUM_ITEMS_KEY = "num.items";
	public static final String SLEEP_KEY = "sleep";
	public static final int DEFAULT_NUM_ITEMS = 1_000;
	private static final Logger LOG = LoggerFactory.getLogger(QueryGeneratorSource.class);
	private final int numItems;
	private final long sleep;
	private volatile boolean isRunning = true;

	public QueryGeneratorSource(ParameterTool params) {
		this.numItems = params.getInt(NUM_ITEMS_KEY, DEFAULT_NUM_ITEMS);
		this.sleep = Math.min(1000, params.getLong(SLEEP_KEY, 1) * 100);
	}

	@Override
	public void run(SourceContext<Query> ctx) throws Exception {
		ThreadLocalRandom rnd = ThreadLocalRandom.current();

		LOG.info("Starting query generator for {} items and {} sleep", numItems, sleep);

		while (isRunning) {
			String itemId = "item_" + (rnd.nextInt(numItems) + 1);
			synchronized (ctx.getCheckpointLock()) {
				ctx.collect(new Query(rnd.nextLong(Long.MAX_VALUE), System.currentTimeMillis(), itemId));
			}
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
