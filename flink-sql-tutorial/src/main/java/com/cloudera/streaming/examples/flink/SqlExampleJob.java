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

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class SqlExampleJob {

	public static void main(String[] args) throws Exception {

		TableEnvironment env = createTableEnv();
		env.sqlUpdate("CREATE TABLE ItemTransactions ( " +
				"transactionId    BIGINT, " +
				"ts    BIGINT, " +
				"itemId    STRING, " +
				"quantity INT, " +
				"event_time AS CAST(from_unixtime(floor(ts/1000)) AS TIMESTAMP(3)), " +
				"WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND " +
				") WITH ( " +
				"'connector.type'     = 'kafka', " +
				"'connector.version'  = 'universal', " +
				"'connector.topic'    = 'transaction.log.1', " +
				"'connector.startup-mode' = 'earliest-offset', " +
				"'connector.properties.bootstrap.servers' = 'gyula-1.gce.cloudera.com:9092', " +
				"'format.type' = 'json' " +
				")");

		env.sqlUpdate("CREATE TABLE LargeTransactions ( " +
				"transactionId    BIGINT, " +
				"ts    BIGINT, " +
				"itemId    STRING, " +
				"quantity INT " +
				") WITH ( " +
				"'connector.type'     = 'kafka', " +
				"'connector.version'  = 'universal', " +
				"'connector.topic'    = 'large.transaction.log.1', " +
				"'connector.properties.bootstrap.servers' = 'gyula-1.gce.cloudera.com:9092', " +
				"'format.type' = 'json' " +
				")");

		env.sqlUpdate("INSERT INTO LargeTransactions " +
				"SELECT transactionId, ts, itemId, quantity FROM ItemTransactions " +
				"WHERE " +
				"quantity > 100");

		env.execute("test");
	}

	public static TableEnvironment createTableEnv() {
		EnvironmentSettings settings = EnvironmentSettings
				.newInstance()
				.useBlinkPlanner()
				.inStreamingMode()
				.build();

		TableEnvironment tableEnv = TableEnvironment.create(settings);

		return tableEnv;
	}

}
