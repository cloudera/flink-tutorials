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

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.client.cli.CliFrontend;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.encrypttool.EncryptTool;

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
import static com.cloudera.streaming.examples.flink.Constants.MASK;
import static com.cloudera.streaming.examples.flink.Constants.SENSITIVE_KEYS_KEY;

/**
 * Utility functions for the security tutorial.
 */
public final class Utils {

	private Utils() {
		throw new UnsupportedOperationException("Utils should not be instantiated");
	}

	private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

	public static ParameterTool parseArgs(String[] args) throws IOException {

		// Processing job properties
		ParameterTool params = ParameterTool.fromArgs(args);
		if (params.has(K_PROPERTIES_FILE)) {
			params = ParameterTool.fromPropertiesFile(params.getRequired(K_PROPERTIES_FILE)).mergeWith(params);
		}

		LOG.info("### Job parameters:");
		for (String key : params.getProperties().stringPropertyNames()) {
			LOG.info("Job Param: {}={}", key, isSensitive(key, params) ? MASK : params.get(key));
		}
		return params;
	}

	public static Properties readKafkaProperties(ParameterTool params) {
		Properties properties = new Properties();
		for (String key : params.getProperties().stringPropertyNames()) {
			if (key.startsWith(KAFKA_PREFIX)) {
				properties.setProperty(key.substring(KAFKA_PREFIX.length()), isSensitive(key, params) ? decrypt(params.get(key)) : params.get(key));
			}
		}

		LOG.info("### Kafka parameters:");
		for (String key : properties.stringPropertyNames()) {
			LOG.info("Loading configuration property: {}, {}", key, isSensitive(key, params) ? MASK : properties.get(key));
		}
		return properties;
	}

	public static Map<String, Object> readSchemaRegistryProperties(ParameterTool params) {

		//Setting up schema registry client

		Map<String, Object> schemaRegistryConf = new HashMap<>();
		schemaRegistryConf.put(K_SCHEMA_REG_URL, params.getRequired(K_SCHEMA_REG_URL));

		if (params.getRequired(K_SCHEMA_REG_URL).startsWith("https")) {
			Map<String, String> sslClientConfig = new HashMap<>();
			String sslKey = K_SCHEMA_REG_SSL_CLIENT_KEY + "." + K_TRUSTSTORE_PATH;
			sslClientConfig.put(K_TRUSTSTORE_PATH, isSensitive(sslKey, params) ? decrypt(params.getRequired(sslKey)) : params.getRequired(sslKey));
			sslKey = K_SCHEMA_REG_SSL_CLIENT_KEY + "." + K_TRUSTSTORE_PASSWORD;
			sslClientConfig.put(K_TRUSTSTORE_PASSWORD, isSensitive(sslKey, params) ? decrypt(params.getRequired(sslKey)) : params.getRequired(sslKey));
			sslClientConfig.put(K_KEYSTORE_PASSWORD, ""); //ugly hack needed for SchemaRegistryClient

			schemaRegistryConf.put(K_SCHEMA_REG_SSL_CLIENT_KEY, sslClientConfig);
		}

		LOG.info("### Schema Registry parameters:");
		for (String key : schemaRegistryConf.keySet()) {
			LOG.info("Schema Registry param: {}={}", key, isSensitive(key, params) ? MASK : schemaRegistryConf.get(key));
		}
		return schemaRegistryConf;
	}

	public static boolean isSensitive(String key, ParameterTool params) {
		Preconditions.checkNotNull(key, "key is null");
		final String value = params.get(SENSITIVE_KEYS_KEY);
		if (value == null) {
			return false;
		}
		String keyInLower = key.toLowerCase();
		String[] sensitiveKeys = value.split(",");

		for (int i = 0; i < sensitiveKeys.length; ++i) {
			String hideKey = sensitiveKeys[i];
			if (keyInLower.length() >= hideKey.length() && keyInLower.contains(hideKey)) {
				return true;
			}
		}
		return false;
	}

	public static String decrypt(String input) {
		Preconditions.checkNotNull(input, "key is null");
		return EncryptTool.getInstance(getConfiguration()).decrypt(input);
	}

	public static Configuration getConfiguration() {
		return ConfigHolder.INSTANCE;
	}

	private static class ConfigHolder {
		static final Configuration INSTANCE = GlobalConfiguration.loadConfiguration(CliFrontend.getConfigurationDirectoryFromEnv());
	}

}
