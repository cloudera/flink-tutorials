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

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Common serialization logic for JSON schemas.
 */
public abstract class JsonKafkaSerializationSchema<T> implements KafkaSerializationSchema<T> {

  protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  protected final String topic;

  protected JsonKafkaSerializationSchema(String topic) {
    this.topic = topic;
  }

  protected abstract byte[] getKeyAsBytes(T obj);

  @Override
  public ProducerRecord<byte[], byte[]> serialize(T obj, Long ts) {
    try {
      byte[] key = getKeyAsBytes(obj);
      byte[] val = OBJECT_MAPPER.writeValueAsBytes(obj);

      return new ProducerRecord<>(topic, key, val);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
