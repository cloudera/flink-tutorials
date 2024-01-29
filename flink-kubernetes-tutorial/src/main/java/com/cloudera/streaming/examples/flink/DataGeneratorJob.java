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

package com.cloudera.streaming.examples.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Simple Flink job that generates increasing numbers and writes them into a file stored on disk.
 */
public class DataGeneratorJob {

    public static void main(String[] args) throws Exception {

        // Read the parameters from the commandline
        ParameterTool params = ParameterTool.fromArgs(args);
        final int rowsPerSec = params.getInt("rowsPerSec", 10);
        final String outputPath = params.get("outputPath", "/tmp");

        // Create and configure the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000);

        // Define our source
        DataGeneratorSource<String> dataGenSource = new DataGeneratorSource<>(
                String::valueOf,
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(rowsPerSec),
                Types.STRING
        );

        final FileSink<String> fileSink = FileSink
                .forRowFormat(new Path(outputPath), new SimpleStringEncoder<String>("UTF-8"))
                .build();

        env.fromSource(dataGenSource, WatermarkStrategy.noWatermarks(), "datagen")
                .name("DataGenSource")
                .sinkTo(fileSink)
                .name("FileSink");


        env.execute("DataGeneratorJob");
    }
}
