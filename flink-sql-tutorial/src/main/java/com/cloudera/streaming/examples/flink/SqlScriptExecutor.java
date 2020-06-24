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

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.util.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Small example application executing an sql script file in Flink.
 */
public class SqlScriptExecutor {

	private static final Logger LOG = LoggerFactory.getLogger(SqlScriptExecutor.class);
	private static final String HIVE_CATALOG = "hive";
	private static final String HIVE_DATABASE = "default";
	private static final String HIVE_CONF_DIR = "/etc/hive/conf";
	private static final String HIVE_VERSION = "3.1.2000";

	public static void main(String[] args) throws Exception {

		HiveCatalog hiveCatalog = new HiveCatalog(HIVE_CATALOG, HIVE_DATABASE, HIVE_CONF_DIR, HIVE_VERSION);
		StreamTableEnvironment env = createTableEnv();
		env.registerCatalog(HIVE_CATALOG, hiveCatalog);

		File script = new File(args[0]);
		String[] commands = FileUtils.readFileUtf8(script).split(";");

		for (String command : commands) {
			if (command.trim().isEmpty()) {
				continue;
			}

			LOG.info("Executing SQL statement: {}", command.trim());
			env.sqlUpdate(command.trim());
		}

		env.execute("SQL Script: " + script.getName());
	}

	public static StreamTableEnvironment createTableEnv() {
		EnvironmentSettings settings = EnvironmentSettings
				.newInstance()
				.useBlinkPlanner()
				.inStreamingMode()
				.build();

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		StreamTableEnvironment tableEnv = StreamTableEnvironment
				.create(env, settings);

		return tableEnv;
	}

}
