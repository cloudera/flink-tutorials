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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Transaction serialization schema for running the example with kafka.
 */
public class TransactionSchema implements KeyedSerializationSchema<ItemTransaction>, DeserializationSchema<ItemTransaction> {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	@Override
	public byte[] serializeKey(ItemTransaction t) {
		return t.itemId.getBytes(StandardCharsets.UTF_8);
	}

	@Override
	public byte[] serializeValue(ItemTransaction t) {
		try {
			return OBJECT_MAPPER.writeValueAsBytes(t);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public String getTargetTopic(ItemTransaction t) {
		return null;
	}

	@Override
	public ItemTransaction deserialize(byte[] message) {
		try {
			return OBJECT_MAPPER.readValue(message, ItemTransaction.class);
		} catch (IOException e) {
			return null;
		}
	}

	@Override
	public boolean isEndOfStream(ItemTransaction nextElement) {
		return false;
	}

	@Override
	public TypeInformation<ItemTransaction> getProducedType() {
		return new TypeHint<ItemTransaction>() {
		}.getTypeInfo();
	}
}
