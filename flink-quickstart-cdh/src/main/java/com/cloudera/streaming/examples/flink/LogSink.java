package com.cloudera.streaming.examples.flink;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogSink implements SinkFunction {

    private static final Logger LOG = LoggerFactory.getLogger(LogSink.class);

    @Override
    public void invoke(Object value, Context context) throws Exception {
        if (LOG.isInfoEnabled() && value != null) {
            LOG.info(value.toString());
        }
    }
}
