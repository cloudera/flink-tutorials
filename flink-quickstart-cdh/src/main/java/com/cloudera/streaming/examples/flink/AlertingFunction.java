package com.cloudera.streaming.examples.flink;

import com.cloudera.streaming.examples.flink.types.HeapAlert;
import com.cloudera.streaming.examples.flink.types.HeapStats;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

public class AlertingFunction implements FlatMapFunction<HeapStats, HeapAlert> {

    public static final String ALERT_MASK = "alertMask";
    private final String alertMask;

    public AlertingFunction(ParameterTool params) {
        alertMask = params.get(ALERT_MASK, "42");
    }

    @Override
    public void flatMap(HeapStats stats, Collector<HeapAlert> out) throws Exception {
        if (Double.toString(stats.ratio).contains(alertMask)) {
            out.collect(HeapAlert.maskRatioMatch(alertMask, stats));
        }
    }

}
