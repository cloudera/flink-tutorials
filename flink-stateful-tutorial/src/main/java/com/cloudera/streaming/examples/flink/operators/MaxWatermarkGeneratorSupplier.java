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

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * Watermark implementation that emits {@link Watermark#MAX_WATERMARK}, basically removing
 * this stream from watermark computation.
 *
 * <p>Should only be used on streams that won't be aggregated to the window.
 */
public class MaxWatermarkGeneratorSupplier<T> implements WatermarkGeneratorSupplier<T> {

	@Override
	public WatermarkGenerator<T> createWatermarkGenerator(Context context) {
		return new WatermarkGenerator<T>() {
			@Override
			public void onEvent(T t, long l, WatermarkOutput watermarkOutput) {
				watermarkOutput.emitWatermark(Watermark.MAX_WATERMARK);
			}

			@Override
			public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
				watermarkOutput.emitWatermark(Watermark.MAX_WATERMARK);
			}
		};
	}
}
