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

import com.cloudera.streaming.examples.flink.data.Message;
import org.apache.flink.formats.avro.registry.cloudera.SchemaRegistrySerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;

public class SchemaRegistrySerializationSchemaTest {

    /*
        Required System properties:
        -Djava.security.auth.login.config=/Users/matyasorhidi/Temp/schemareg/schema.registry.jaas.conf
        -DtrustStorePath=/Users/matyasorhidi/Temp/schemareg/cm-auto-global_truststore.jks
        -DtrustStorePassword=changeit
        -Dschema.registry.url=https://morhidi-sec-1.gce.cloudera.com:7790/api/v1

        > cat /Users/matyasorhidi/Temp/schemareg/schema.registry.jaas.conf
        RegistryClient {
          com.sun.security.auth.module.Krb5LoginModule required
          useKeyTab=true
          keyTab="/Users/matyasorhidi/Temp/schemareg/test.keytab"
          principal="test"
          doNotPrompt=true;
        };
     */

    public static void main(String[] args) {
        Map<String, String> sslConfig = new HashMap<>();
        sslConfig.put("trustStorePath", System.getProperty("trustStorePath"));
        sslConfig.put("trustStorePassword", System.getProperty("trustStorePassword"));
        sslConfig.put("keyStorePassword", ""); //ugly hack needed for SchemaRegistrySerializationSchema

        Map<String, Object> config = new HashMap<>();
        config.put("schema.registry.url", System.getProperty("schema.registry.url"));
        config.put("schema.registry.client.ssl", sslConfig);

        SchemaRegistrySerializationSchema<String, Message, Message> schema = SchemaRegistrySerializationSchema.<Message>
                builder("dummy")
                .setConfig(config)
                .setKey(Message::getId)
                .build();

        ProducerRecord<byte[], byte[]> serialize = schema.serialize(new Message("sdf", "sdf", "sdf"), 1L);

        System.out.println(serialize);

    }

}
