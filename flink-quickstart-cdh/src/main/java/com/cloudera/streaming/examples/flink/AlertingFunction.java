package com.cloudera.streaming.examples.flink;

import com.cloudera.streaming.examples.flink.types.HeapAlert;
import com.cloudera.streaming.examples.flink.types.HeapStats;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

public class AlertingFunction implements FlatMapFunction<HeapStats, HeapAlert> {

    public static final String WARNING_THRESHOLD_KEY = "warningThreshold";
    public static final String CRITICAL_THRESHOLD_KEY = "criticalThreshold";

    private final double warningThreshold;
    private final double criticalThreshold;

    public AlertingFunction(ParameterTool params) {
        warningThreshold = params.getDouble(WARNING_THRESHOLD_KEY, 0.5);
        criticalThreshold = params.getDouble(CRITICAL_THRESHOLD_KEY, 0.8);

        if (warningThreshold >= criticalThreshold) {
            throw new IllegalArgumentException("Warning threshold must be lower than critical threshold");
        }
    }

    @Override
    public void flatMap(HeapStats stats, Collector<HeapAlert> out) throws Exception {
        if (stats.area.equals(HeapStats.OLD_GEN)) {
            if (stats.ratio >= criticalThreshold) {
                out.collect(HeapAlert.criticalOldGen(stats));
            } else if (stats.ratio >= warningThreshold) {
                out.collect(HeapAlert.GCWarning(stats));
            }
        }
    }

}
