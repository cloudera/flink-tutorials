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

/**
 * Sink implementation that collects elements into {@link BlockingQueue}s so it can be polled
 * efficiently in tests.
 * <p>
 * The logic relies on static fields so it can be only used in unit tests where the flink pipeline
 * runs in the same JVM.
 */
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
