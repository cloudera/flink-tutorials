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
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;

public class HeapMonitorPipeline {

    public static void main(String[] args) throws Exception {

        // Read the parameters from the commandline
        ParameterTool params = ParameterTool.fromArgs(args);
        final boolean clusterExec = params.getBoolean("cluster", false);
        final String output = params.get("output", "hdfs:///tmp/flink-heap-stats");

        // Create and configure the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000);

        // Define our source
        DataStream<HeapMetrics> heapStats = env.addSource(new HeapMonitorSource(100))
                .name("Heap Monitor Source");

        // Define the sink for the whole statistics stream
        if (!clusterExec) {
            // In local execution mode print the stats to stdout
            heapStats.print();
        } else {
            // In cluster execution mode write the stats to HDFS
            final StreamingFileSink<String> sfs = StreamingFileSink
                    .forRowFormat(new Path(output), new SimpleStringEncoder<String>("UTF-8"))
                    .build();

            heapStats.map(stats -> stats.toString()).addSink(sfs).name("HDFS Sink");
        }

        // Detect suspicious events in the statistics stream, defining this as a separate function enables testing
        DataStream<HeapAlert> alertStream = computeHeapAlerts(heapStats, params);

        // Write the output to the log stream, we can direct this to stderr or to Kafka via the log4j configuration
        alertStream.addSink(new LogSink<>()).name("Logger Sink");

        env.execute("HeapMonitor");
    }

    public static DataStream<HeapAlert> computeHeapAlerts(DataStream<HeapMetrics> statsInput, ParameterTool params) {
        return statsInput.flatMap(new AlertingFunction(params)).name("Create Alerts");
    }
}
