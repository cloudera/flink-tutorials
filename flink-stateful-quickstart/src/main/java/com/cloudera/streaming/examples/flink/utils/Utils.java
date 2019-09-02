package com.cloudera.streaming.examples.flink.utils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

import static org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS;

public class Utils {

    public static Properties createKafkaProducerProps(String broker) {
        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");

        return properties;
    }

    public static Properties createKafkaConsumerProps(String broker, String groupId) {
        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, "60000");

        return properties;
    }
}
