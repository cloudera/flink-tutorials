package com.cloudera.streaming.examples.flink.utils.testing;

import org.apache.flink.streaming.api.datastream.DataStream;

public interface ManualSource<T> {

	void sendRecord(T event);

	void sendRecord(T event, long ts);

	void sendWatermark(long ts);

	void markFinished();
	
	DataStream<T> getDataStream();
}
