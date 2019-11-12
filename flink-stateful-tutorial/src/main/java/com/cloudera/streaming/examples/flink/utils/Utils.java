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

package com.cloudera.streaming.examples.flink.utils;

import org.apache.flink.api.java.utils.ParameterTool;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Utils {

	private static Logger LOG = LoggerFactory.getLogger(Utils.class);

	public static final String KAFKA_PREFIX = "kafka.";

	public static Properties readKafkaProperties(ParameterTool params, boolean consumer) {
		Properties properties = new Properties();

		if (consumer) {
			properties.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
					"com.hortonworks.smm.kafka.monitoring.interceptors.MonitoringConsumerInterceptor");
		} else {
			properties.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
					"com.hortonworks.smm.kafka.monitoring.interceptors.MonitoringProducerInterceptor");
		}

		for (String key : params.getProperties().stringPropertyNames()) {
			if (key.startsWith(KAFKA_PREFIX)) {
				properties.setProperty(key.substring(KAFKA_PREFIX.length()), params.get(key));
			}
		}

		LOG.info("### Kafka parameters:");
		for (String key : properties.stringPropertyNames()) {
			LOG.info("Kafka param: {}={}", key, properties.get(key));
		}
		return properties;
	}
}
