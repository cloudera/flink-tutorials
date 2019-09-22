package com.cloudera.streaming.examples.flink.utils.testing;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

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
