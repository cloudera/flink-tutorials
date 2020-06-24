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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import org.junit.Test;

/**
 * Simple test for Flink sql.
 */
public class SqlTest {

	@Test
	public void test() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = createTableEnv(env);

		DataStream<Tuple2<Long, String>> ds = env.fromElements(
				Tuple2.of(1L, "a"),
				Tuple2.of(2L, "b"),
				Tuple2.of(3L, "c")
		);

		Table table = tableEnv.fromDataStream(ds, "id, name");

		Table result = tableEnv.sqlQuery("SELECT * from " + table);

		tableEnv.toAppendStream(result, ds.getType()).print();
		env.execute("test");
	}

	public static StreamTableEnvironment createTableEnv(StreamExecutionEnvironment env) {
		EnvironmentSettings settings = EnvironmentSettings
				.newInstance()
				.useBlinkPlanner()
				.inStreamingMode()
				.build();

		StreamTableEnvironment tableEnv = StreamTableEnvironment
				.create(env, settings);

		return tableEnv;
	}

}
