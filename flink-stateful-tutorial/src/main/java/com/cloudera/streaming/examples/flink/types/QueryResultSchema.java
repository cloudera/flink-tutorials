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

package com.cloudera.streaming.examples.flink.types;

import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;

/**
 * Query result serialization schema for running the example with kafka.
 */
public class QueryResultSchema implements KeyedSerializationSchema<QueryResult> {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	@Override
	public byte[] serializeKey(QueryResult res) {
		return String.valueOf(res.queryId).getBytes(StandardCharsets.UTF_8);
	}

	@Override
	public byte[] serializeValue(QueryResult res) {
		try {
			return OBJECT_MAPPER.writeValueAsBytes(res);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public String getTargetTopic(QueryResult res) {
		return null;
	}

}
