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

import com.cloudera.streaming.examples.flink.types.HeapAlert;
import com.cloudera.streaming.examples.flink.types.HeapMetrics;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

public class AlertingFunction implements FlatMapFunction<HeapMetrics, HeapAlert> {

    public static final String ALERT_MASK = "alertMask";
    private final String alertMask;

    public AlertingFunction(ParameterTool params) {
        alertMask = params.get(ALERT_MASK, "42");
    }

    @Override
    public void flatMap(HeapMetrics stats, Collector<HeapAlert> out) throws Exception {
        if (Double.toString(stats.ratio).contains(alertMask)) {
            out.collect(HeapAlert.maskRatioMatch(alertMask, stats));
        }
    }

}
