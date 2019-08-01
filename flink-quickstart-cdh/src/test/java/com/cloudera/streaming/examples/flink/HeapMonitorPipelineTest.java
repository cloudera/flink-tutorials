package com.cloudera.streaming.examples.flink;

import com.cloudera.streaming.examples.flink.types.HeapAlert;
import com.cloudera.streaming.examples.flink.types.HeapStats;
import org.apache.commons.compress.utils.Sets;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class HeapMonitorPipelineTest {

    static Set<HeapAlert> testOutput = new HashSet<>();

    @Test
    public void testPipeline() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        HeapStats edenStat = testStats(HeapStats.EDEN, 0.99);
        HeapStats warning = testStats(HeapStats.OLD_GEN, 0.6);
        HeapStats normalOld = testStats(HeapStats.OLD_GEN, 0.45);
        HeapStats criticalOld = testStats(HeapStats.OLD_GEN, 0.99);

        DataStreamSource<HeapStats> testInput = env.fromElements(edenStat, warning, normalOld, criticalOld);
        HeapMonitorPipeline.computeHeapAlerts(testInput).addSink(new SinkFunction<HeapAlert>() {
            @Override
            public void invoke(HeapAlert value) throws Exception {
                testOutput.add(value);
            }
        }).setParallelism(1);

        env.execute();

        assertEquals(Sets.newHashSet(HeapAlert.GCWarning(warning), HeapAlert.criticalOldGen(criticalOld)), testOutput);
    }

    private HeapStats testStats(String area, double ratio) {
        return new HeapStats(area, 0, 0, ratio, 0, "testhost");
    }
}
