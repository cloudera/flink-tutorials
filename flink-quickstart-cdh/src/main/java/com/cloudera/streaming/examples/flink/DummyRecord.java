package com.cloudera.streaming.examples.flink;

public class DummyRecord {

    public String taskName;
    public Integer index;
    public String hostname;
    public Long timestamp;

    public DummyRecord() {
    }

    public DummyRecord(String taskName, Integer index, String hostname, Long timestamp) {
        this.taskName = taskName;
        this.index = index;
        this.hostname = hostname;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "DummyRecord{" +
                "taskName='" + taskName + '\'' +
                ", index=" + index +
                ", hostname='" + hostname + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
