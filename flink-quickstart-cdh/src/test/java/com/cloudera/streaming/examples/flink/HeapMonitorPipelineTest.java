package com.cloudera.streaming.examples.flink;

import com.cloudera.streaming.examples.flink.types.HeapStats;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;
import scala.collection.immutable.Stream;

public class HeapMonitorPipelineTest {

    @Test
    public void testPipeline() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        HeapStats normal = new HeapStats("as", 1l, 2l, 0.1, 2, "testhost");
    }
}
