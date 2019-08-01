package com.cloudera.streaming.examples.flink;

import com.cloudera.streaming.examples.flink.types.HeapAlert;
import com.cloudera.streaming.examples.flink.types.HeapStats;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class AlertingFunction implements FlatMapFunction<HeapStats, HeapAlert> {

    @Override
    public void flatMap(HeapStats stats, Collector<HeapAlert> out) throws Exception {
        if (stats.area.equals(HeapStats.OLD_GEN)) {
            if (stats.ratio >= 0.8) {
                out.collect(HeapAlert.criticalOldGen(stats));
            } else if (stats.ratio >= 0.5) {
                out.collect(HeapAlert.GCWarning(stats));
            }
        }
    }

}
