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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import com.cloudera.streaming.examples.flink.operators.ItemTransactionGeneratorSource;
import com.cloudera.streaming.examples.flink.operators.QueryGeneratorSource;
import com.cloudera.streaming.examples.flink.types.ItemTransaction;
import com.cloudera.streaming.examples.flink.types.Query;
import com.cloudera.streaming.examples.flink.types.QuerySchema;
import com.cloudera.streaming.examples.flink.types.TransactionSchema;
import com.cloudera.streaming.examples.flink.utils.Utils;

/**
 * Simple Flink job that generates {@link ItemTransaction} data to Kafka.
 */
public class KafkaDataGeneratorJob {

	private static final String GENERATE_QUERIES = "generate.queries";

	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			throw new RuntimeException("Path to the properties file is expected as the only argument.");
		}
		ParameterTool params = ParameterTool.fromPropertiesFile(args[0]);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<ItemTransaction> generatedInput =
				env.addSource(new ItemTransactionGeneratorSource(params))
						.name("Item Transaction Generator");

		String transactionTopic = params.getRequired(KafkaItemTransactionJob.TRANSACTION_INPUT_TOPIC_KEY);
		FlinkKafkaProducer<ItemTransaction> kafkaSink = new FlinkKafkaProducer<>(
				transactionTopic,
				new TransactionSchema(transactionTopic),
				Utils.readKafkaProperties(params),
				FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);

		generatedInput.keyBy(t -> t.itemId).addSink(kafkaSink).name("Transaction Kafka Sink");

		if (params.getBoolean(GENERATE_QUERIES, false)) {
			DataStream<Query> queries = env.addSource(new QueryGeneratorSource(params))
					.name("Query Generator");

			String queryTopic = params.getRequired(KafkaItemTransactionJob.QUERY_INPUT_TOPIC_KEY);
			FlinkKafkaProducer<Query> querySink = new FlinkKafkaProducer<>(
					queryTopic,
					new QuerySchema(queryTopic),
					Utils.readKafkaProperties(params),
					FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);

			queries.keyBy(q -> q.itemId).addSink(querySink).name("Query Kafka Sink");
		}

		env.execute("Kafka Data generator");
	}
}
