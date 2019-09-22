package com.cloudera.streaming.examples.flink;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import com.cloudera.streaming.examples.flink.operators.ItemTransactionGeneratorSource;
import com.cloudera.streaming.examples.flink.types.ItemTransaction;
import com.cloudera.streaming.examples.flink.types.TransactionSchema;
import com.cloudera.streaming.examples.flink.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class KafkaDataGeneratorJob {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaDataGeneratorJob.class);

	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			throw new RuntimeException("Path to the properties file is expected as the only argument.");
		}
		ParameterTool params = ParameterTool.fromPropertiesFile(args[0]);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<ItemTransaction> generatedInput =
				env.addSource(new ItemTransactionGeneratorSource(params))
						.name("Item Transaction Generator");

		FlinkKafkaProducer<ItemTransaction> kafkaSink = new FlinkKafkaProducer<>(
				params.getRequired(KafkaItemTransactionJob.TRANSACTION_INPUT_TOPIC_KEY),
				new TransactionSchema(),
				Utils.createKafkaProducerProps(params.get(KafkaItemTransactionJob.KAFKA_BROKERS_KEY)),
				Optional.empty());

		generatedInput.keyBy("itemId").addSink(kafkaSink).name("Transaction Kafka Sink");
		env.execute("Kafka Data generator");
	}

}
