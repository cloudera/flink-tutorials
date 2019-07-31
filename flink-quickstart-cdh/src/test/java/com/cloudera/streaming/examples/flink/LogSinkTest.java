package com.cloudera.streaming.examples.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogSinkTest {

    private static final Logger LOG = LoggerFactory.getLogger(LogSinkTest.class);

    @Test
    public void testLogSink() throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> stream = env.fromElements("foo", "bar");
        stream.addSink(new LogSink());
        env.execute("Flink Streaming Java API Skeleton");

    }

}
