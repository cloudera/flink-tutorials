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

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * Watermark implementation that emits Long.MAX_VALUE as watermark and ts, basically removing
 * this stream from watermark computation.
 * <p>
 * Should only be used on streams that won't be aggregated to the window.
 */
public final class MaxWatermark<T> implements AssignerWithPeriodicWatermarks<T> {
	private static final long serialVersionUID = 1L;

	@Override
	public long extractTimestamp(T input, long ts) {
		return Long.MAX_VALUE;
	}

	@Override
	public Watermark getCurrentWatermark() {
		return new Watermark(Long.MAX_VALUE);
	}
}
