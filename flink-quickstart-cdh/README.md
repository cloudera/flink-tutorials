# Flink Quickstart Application

The purpose of the Flink Quickstart Application is to have a working boilerplate code for a Flink project on top of CDH. The application demonstrates some basic capabilities of DataStream API available in Flink. It collects basic heap statistics of the JVM it's running on and stores the collected data on HDFS. In case the heap size is close to the maximum it triggers an alert message to the log. 

```
TODO add sample log entries here
```

## Usage

```
git clone https://github.infra.cloudera.com/morhidi/flink-ref.git
```
## Importing the project to IntelliJ

## Testing the project from IntelliJ



## Running the complied example on a remote Cluster

### Prerequisites

### B
```
git clone https://github.infra.cloudera.com/morhidi/flink-ref.git
cd flink-quickstart-cdh
mvn clean package
```
