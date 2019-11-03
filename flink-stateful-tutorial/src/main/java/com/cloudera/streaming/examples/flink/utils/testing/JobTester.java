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

package com.cloudera.streaming.examples.flink.utils.testing;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

/**
 * Helper class for executing Flink streaming unit-test style pipeline tests.
 * The {@link #createManualSource(StreamExecutionEnvironment, TypeInformation)} method can be used
 * to create data sources that we can use to send elements one by one after starting the pipeline and
 * the {@link CollectingSink} can be used to retrieve output and make assertions.
 * <p>
 * The {@link #startTest(StreamExecutionEnvironment)} method triggers job execution in a background thread so
 * we can add our testing logic, which should be followed by {@link #stopTest()} that shuts down the job after
 * which we can make our final output checks.
 */
public class JobTester {

	private static final Semaphore finished = new Semaphore(1);
	private static final List<ManualSource<?>> manualStreams = new ArrayList<>();
	private static StreamExecutionEnvironment currentEnv;

	public static <T> ManualSource<T> createManualSource(StreamExecutionEnvironment env, TypeInformation<T> type) {
		if (currentEnv != null && currentEnv != env) {
			throw new RuntimeException("JobTester can only be used on 1 env at a time");
		}
		JobTester.currentEnv = env;
		ManualFlinkSource<T> manualFlinkSource = new ManualFlinkSource<>(env, type);
		manualStreams.add(manualFlinkSource);
		return manualFlinkSource;
	}

	public static void startTest(StreamExecutionEnvironment env) throws Exception {
		if (env != currentEnv) {
			throw new RuntimeException("JobTester can only be used on 1 env at a time");
		}

		if (!(env instanceof LocalStreamEnvironment)) {
			throw new RuntimeException("Tester can only run in a local environment");
		}

		finished.acquire();
		new Thread(() -> {
			try {
				env.execute();
			} catch (Exception e) {
				throw new RuntimeException(e);
			} finally {
				finished.release();
			}
		}).start();
	}

	public static void stopTest() throws Exception {
		manualStreams.forEach(s -> s.markFinished());
		finished.acquire();
		finished.release();
		manualStreams.clear();
		currentEnv = null;
	}
}
