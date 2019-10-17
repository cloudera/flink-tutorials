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

package com.cloudera.streaming.examples.flink;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import com.cloudera.streaming.examples.flink.types.HeapMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.net.InetAddress;

public class HeapMonitorSource extends RichParallelSourceFunction<HeapMetrics> {

	private static final Logger LOG = LoggerFactory.getLogger(HeapMonitorSource.class);

	private final long sleepMillis;
	private volatile boolean running = true;

	public HeapMonitorSource(long sleepMillis) {
		this.sleepMillis = sleepMillis;
	}

	@Override
	public void run(SourceFunction.SourceContext<HeapMetrics> sourceContext) throws Exception {
		LOG.info("starting HeapMonitorSource");

		int subtaskIndex = this.getRuntimeContext().getIndexOfThisSubtask();
		String hostname = InetAddress.getLocalHost().getHostName();

		while (running) {
			Thread.sleep(sleepMillis);

			for (MemoryPoolMXBean mpBean : ManagementFactory.getMemoryPoolMXBeans()) {
				if (mpBean.getType() == MemoryType.HEAP) {
					MemoryUsage memoryUsage = mpBean.getUsage();
					long used = memoryUsage.getUsed();
					long max = memoryUsage.getMax();

					synchronized (sourceContext.getCheckpointLock()) {
						sourceContext.collect(new HeapMetrics(mpBean.getName(), used, max, (double) used / max, subtaskIndex, hostname));
					}
				}
			}
		}
	}

	@Override
	public void cancel() {
		this.running = false;
	}
}
