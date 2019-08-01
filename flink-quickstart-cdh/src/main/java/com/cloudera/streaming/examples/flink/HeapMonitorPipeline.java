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
import com.cloudera.streaming.examples.flink.types.HeapStats;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.util.Collector;


public class HeapMonitorPipeline {

    public static void main(String[] args) throws Exception {

        ParameterTool paramTool = ParameterTool.fromArgs(args);

        final String output = paramTool.get("output", "/tmp/flink-quickstart-cdh/alerts");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000);

        DataStream<HeapStats> statsInput = env.addSource(new HeapMonitorSource(100))
                .name("Heap Monitor Source");
        final StreamingFileSink<String> sfs = StreamingFileSink
                .forRowFormat(new Path(output), new SimpleStringEncoder<String>("UTF-8"))
                .build();
        statsInput.map(stats -> stats.toString()).addSink(sfs);


        DataStream<HeapAlert> alertStream = computeHeapAlerts(statsInput);
        alertStream.addSink(new LogSink());


        env.execute("HeapMonitor");
    }

    public static DataStream<HeapAlert> computeHeapAlerts(DataStream<HeapStats> statsInput) {
        return statsInput.flatMap(new AlertingFunction()).name("Create Alerts");
    }
}
