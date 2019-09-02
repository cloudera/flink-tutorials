package com.cloudera.streaming.examples.flink.operators;

import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;

import java.util.Arrays;
import java.util.Random;

public class HashingKafkaPartitioner<T> extends FlinkKafkaPartitioner<T> {

	private final Random rnd = new Random();

	@Override
	public int partition(T next, byte[] serializedKey, byte[] serializedValue, String topic, int[] partitions) {
		int numPartitions = partitions.length;
		if (serializedKey == null) {
			return rnd.nextInt(numPartitions);
		} else {
			return Math.abs(Arrays.hashCode(serializedKey)) % numPartitions;
		}
	}

}
