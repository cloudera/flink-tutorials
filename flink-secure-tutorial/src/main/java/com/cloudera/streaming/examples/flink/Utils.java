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

import org.apache.flink.api.java.utils.ParameterTool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.cloudera.streaming.examples.flink.Constants.KAFKA_PREFIX;
import static com.cloudera.streaming.examples.flink.Constants.K_KEYSTORE_PASSWORD;
import static com.cloudera.streaming.examples.flink.Constants.K_PROPERTIES_FILE;
import static com.cloudera.streaming.examples.flink.Constants.K_SCHEMA_REG_SSL_CLIENT_KEY;
import static com.cloudera.streaming.examples.flink.Constants.K_SCHEMA_REG_URL;
import static com.cloudera.streaming.examples.flink.Constants.K_TRUSTSTORE_PASSWORD;
import static com.cloudera.streaming.examples.flink.Constants.K_TRUSTSTORE_PATH;

public class Utils {

	private static Logger LOG = LoggerFactory.getLogger(Utils.class);

	public static ParameterTool parseArgs(String[] args) throws IOException {

		// Processing job properties
		ParameterTool params = ParameterTool.fromArgs(args);
		if (params.has(K_PROPERTIES_FILE)) {
			params = ParameterTool.fromPropertiesFile(params.getRequired(K_PROPERTIES_FILE)).mergeWith(params);
		}

		LOG.info("### Job parameters:");
		for (String key : params.getProperties().stringPropertyNames()) {
			LOG.info("Job Param: {}={}", key, params.get(key));
		}

		return params;
	}

	public static Properties readKafkaProperties(ParameterTool params) {
		Properties properties = new Properties();
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

	public static Map<String, Object> readSchemaRegistryProperties(ParameterTool params) {
		//Setting up schema registry client
		Map<String, String> sslClientConfig = new HashMap<>();
		sslClientConfig.put(K_TRUSTSTORE_PATH, params.getRequired(K_SCHEMA_REG_SSL_CLIENT_KEY + "." + K_TRUSTSTORE_PATH));
		sslClientConfig.put(K_TRUSTSTORE_PASSWORD, params.getRequired(K_SCHEMA_REG_SSL_CLIENT_KEY + "." + K_TRUSTSTORE_PASSWORD));
		sslClientConfig.put(K_KEYSTORE_PASSWORD, ""); //ugly hack needed for SchemaRegistryClient

		Map<String, Object> schemaRegistryConf = new HashMap<>();
		schemaRegistryConf.put(K_SCHEMA_REG_URL, params.getRequired(K_SCHEMA_REG_URL));
		schemaRegistryConf.put(K_SCHEMA_REG_SSL_CLIENT_KEY, sslClientConfig);

		LOG.info("### Schema Registry parameters:");
		for (String key : schemaRegistryConf.keySet()) {
			LOG.info("Schema Registry param: {}={}", key, schemaRegistryConf.get(key));
		}
		return schemaRegistryConf;
	}

}
