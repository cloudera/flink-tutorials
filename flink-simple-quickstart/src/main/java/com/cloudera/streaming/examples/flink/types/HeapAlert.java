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
 
package com.cloudera.streaming.examples.flink.types;

import java.util.Objects;

public class HeapAlert {

    private static final String MASK_RATIO_MATCH_MSG = " was found in the HeapMetrics ratio.";

    public String message;
    public HeapMetrics triggeringMetrics;

    public HeapAlert() {}

    public HeapAlert(String message, HeapMetrics triggeringMetrics) {
        this.message = message;
        this.triggeringMetrics = triggeringMetrics;
    }

    public static HeapAlert maskRatioMatch(String alertMask, HeapMetrics heapMetrics){
        return new HeapAlert(alertMask + MASK_RATIO_MATCH_MSG, heapMetrics);
    }

    @Override
    public String toString() {
        return "HeapAlert{" +
                "message='" + message + '\'' +
                ", triggeringStats=" + triggeringMetrics +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        HeapAlert heapAlert = (HeapAlert) o;
        return Objects.equals(message, heapAlert.message) &&
                Objects.equals(triggeringMetrics, heapAlert.triggeringMetrics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(message, triggeringMetrics);
    }
}
