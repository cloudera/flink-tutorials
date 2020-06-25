# Stateless Monitoring Application

## Table of contents
1. [Overview](#overview)
2. [Build](#build)
3. [Application logic](#application-logic)
4. [Application structure](#application-structure)
5. [Application main class](#application-main-class)
    + [Creating the stream of heap metrics](#creating-the-stream-of-heap-metrics)
    + [Computing heap alerts](#computing-gc-warnings-and-heap-alerts)
6. [Testing the data pipeline](#testing-the-data-pipeline)
    + [Producing test input](#producing-test-input)
    + [Collecting the test output](#collecting-the-test-output)
7. [Running the application from IntelliJ](#running-the-application-from-intellij)
    + [Logging framework](#logging-framework)
8. [Running the application on a Cloudera cluster](#running-the-application-on-a-cloudera-cluster)
    + [Writing logs to Kafka](#writing-logs-to-kafka)
    + [Writing output to HDFS](#writing-output-to-hdfs)


## Overview
The purpose of the Stateless Monitoring Application tutorial is to provide a self-contained boilerplate code example for a Flink application. You can use this simple tutorial for learning the basics of developing a Flink streaming application.

The application demonstrates basic capabilities of the DataStream API and shares best practices for testing and logging.

By the end of the tutorial, you will be able to:
1. Write and deploy a Flink application
2. Test a Flink application
3. Interact with the Flink logging framework


## Build
Before you start the tutorial, check out the repository and build the artifacts:
```
git clone https://github.com/cloudera/flink-tutorials.git
cd flink-tutorials/flink-simple-tutorial
mvn clean package
```

## Application logic
Before developing a streaming application, you need to decide the logic behind it, in other words: what will the application do? In this use case, we built an application that monitors the metrics of the JVM heap and produces metrics records similar to the following:
```
HeapMetrics{area=PS Old Gen, used=14495768, max=2863661056, ratio=0.005061970574215889, jobId=1, hostname=`<your_hostname>`}
```
This input can be used to demonstrate an alerting solution, for example, to detect critical memory levels. The simple alerting logic is that when the ratio component of the heap statistics contains a specified substring, the application sends an alert. For the sake of simplicity, let's refer to this substring as “alert mask”. You can specify the alert mask to any number within the boundaries of the heap statistics.

For example, if you choose `42` as the alert mask, the application produces the following alert using the above logic:
```
HeapAlert{message='42 was found in the HeapMetrics ratio.', triggeringStats=HeapMetrics{area=PS Old Gen, used=14495768, max=2863661056, ratio=0.005061970574215889, jobId=1, hostname=`<your_hostname>`}}
```

Later, we are demonstrating how you can direct these alerts to sinks, such as stderr or Kafka, through the logging framework, but first we will see how the application structure looks like.

## Application structure

The Heap Monitoring Application has four structural components:

1. **Application main class:** Defines the `StreamExecutionEnvironment` and creates the pipeline.
2. **Data Sources:** Access the heap information and make it available for processing.
3. **Processing operators and flow:** Process the heap usage information to detect critical memory levels and produce the alerts.
4. **Data Sinks:** Store the memory information collected on HDFS and log the alerts to the configured logger.

## Application main class

A Flink application has to define a main class that will be executed on the client side on job submission. The main class will define the application pipeline that is going to be executed on the cluster.

Our main class is the `HeapMonitorPipeline` which contains a main method like any standard Java application. The arguments passed to our main method will be determined by us when we use the Flink client. We use the  `ParameterTool` utility to conveniently pass parameters to our job that we can use in our operator implementations.

The first thing you need to do is to create `StreamExecutionEnvironment`. This class can be used to create datastreams and to configure important job parameters. For example, checkpointing behavior that guarantees data consistency for the application.

```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.enableCheckpointing(10_000);
```

The `getExecutionEnvironment()` static call guarantees that the application pipeline always uses the correct environment based on the location where it is executed. It can run from the IDE, which means a local execution environment. Or from the client for cluster submission, that returns the YARN execution environment.

The rest of the main class defines the application source, the processing flow and the sink followed by the `execute()` call, which will trigger the actual execution of the pipeline either locally or on the cluster.

### Creating the stream of heap metrics

The key data abstraction for every Flink streaming application is the `DataStream` which is a bounded or unbounded flow of records. This application will process memory related information, so we created the `HeapMetrics` class to represent the data records.

The `HeapMetrics` class has a few key properties:

1. It is a public and standalone class (no non-static inner class)
2. It has a public empty constructor
3. All fields are public non-final

With these properties, the class is made to be efficiently serializable by the Flink type system.

These classes are called POJOs (Plain Old Java Objects) in the Flink community. It is possible to structure the class differently by keeping the same serialization properties. For the exact rules, see the [upstream documentation](https://ci.apache.org/projects/flink/flink-docs-stable/dev/types_serialization.html#rules-for-pojo-types). 

Now that we have the record class, we need to produce a `DataStream<HeapMetrics>` of the heap information by adding a source to the `StreamExecutionEnvironment`. Flink comes with a wide variety of built-in sources for different input connectors, but in this case, we built a custom source that collects heap statistics from the host JVM.

The `HeapMonitorSource` class extends the `RichParallelSourceFunction<HeapMetrics>` abstract class, which allows us to use it as a data source.

Let's take a closer look at `HeapMonitorSource` class:

- Every Flink source must implement the `SourceFunction` interface. It provides two methods that is called by the Flink runtime during cluster execution:
  - `run(SourceContext)`: This method should contain the data producer loop. Once it finishes, the source shuts down.
  - `cancel()`: This method is called if the source should terminate before it is finished, for example, to break out early from the `run` method.

- The `RichParallelSourceFunction` extends the basic `SourceFunction` behavior in two important ways:
  - It implements the `ParallelSourceFunction` interface, allowing Flink to create multiple instances of the source logic. One per parallel task instance.
  - It extends the `AbstractRichFunction` abstract class, allowing the implementation to access runtime information, such as parallelism and subtask index, that we will leverage in the source implementation.

The source will continuously poll the heap memory usage of this application, and output it along with some task related information producing the datastream.

### Computing heap alerts

The core data processing logic is encapsulated in the `HeapMonitorPipeline.computeHeapAlerts(DataStream<HeapMetrics> statsInput, ParameterTool params)` method that takes the stream of heap information as input, and should produce a stream of alerts to the output when the conditions are met.

The reason for structuring the code this way is to make the pipeline easily testable later by replacing the production data source with the test data stream.

The core alerting logic is implemented in the `AlertingFunction` class. It is a `FlatMapFunction` that filters out incoming heap statistic objects according to the configured mask, and converts them to `HeapAlerts`. We leverage the `ParameterTool` object coming from the main program entry point to make this alerting mask configurable when using the Flink client later.

## Testing the data pipeline
The business logic of a Flink application consists of one or more operators chained together, which is often called a pipeline. Pipelines can be extracted to static methods and can be easily tested with the JUnit framework. The `HeapMonitorPipelineTest` class gives a sample for this.

A simple JUnit test was written to verify the core application logic. The test is implemented in the `HeapMonitorPipelineTest` and should be regarded as an integration test of the application flow. Even though this pipeline is very simple, you can later use the same idea to test more complex application flows.

The test mimics the application main class with only minor differences:

1. The `StreamExecutionEnvironment` is created in the same way.
2. Instead of using the source implementation, you will use the `env.fromElements(..)` method to pre-populate a `DataStream` with some testing data.
3. You feed this data to the static data processing logic like before.
4. Instead of writing the output anywhere, you need to verify the correctness of the data once the pipeline finished.

### Producing test input

The Flink environment has several methods that can be used to produce data for testing. In this case, we passed the elements of the stream directly, but we could have also implemented a custom source. You have to keep in mind that the order of the messages within the pipeline can change compared to the order at the sources depending on parallelism and partitioning.

### Collecting the test output

To be able to assert the correctness of the output, first you need to get hold of the output elements. In this case, the simplest thing to do is to write a custom data sink that collects the incoming elements into a static `Set<HeapAlert>`.

We have specifically set the parallelism of the data sink to 1 to avoid any concurrency issues that might arise from parallel execution.

As we cannot always force strict ordering for the output elements, we used a `Set` instead of a `List` to compare expected output regardless of the order. This might or might not be the correct approach depending on the application flow, but in this case, this works the best.

## Running the application from IntelliJ

The Application Tutorial is based on the upstream Flink quickstart Maven archetype. The project can be imported into IntelliJ by following the instructions from the [Flink documentation](https://ci.apache.org/projects/flink/flink-docs-stable/dev/projectsetup/java_api_quickstart.html#maven).

To run applications directly from the IDE, you must enable the `add-dependencies-for-IDEA` profile. This ensures that the provided dependencies, that would be otherwise supplied by the runtime environment, are available in this case.

The first step is to simply run the `HeapMonitorPipeline` class from the IDE. Like this, one or more multiple lines are printed to the console:
```
...
13:50:54,524 INFO  com.cloudera.streaming.examples.flink.HeapMonitorSource       - starting HeapMonitorSource
13:50:54,524 INFO  com.cloudera.streaming.examples.flink.HeapMonitorSource       - starting HeapMonitorSource
13:50:54,524 INFO  com.cloudera.streaming.examples.flink.HeapMonitorSource       - starting HeapMonitorSource
13:50:54,524 INFO  com.cloudera.streaming.examples.flink.HeapMonitorSource       - starting HeapMonitorSource
...
```
The number of lines that appear on the console depends on the number of cores of your machine chosen as default parallelism.

Once the application has successfully started, you can observe `HeapMetrics` events printed to stdout in rapid succession:

```
3> HeapMetrics{area=PS Survivor Space, used=10980192, max=11010048, ratio=0.9972882952008929, jobId=2, hostname=`<your_hostname>}`
3> HeapMetrics{area=PS Old Gen, used=14410024, max=2863661056, ratio=0.005032028483192111, jobId=2, hostname=`<your_hostname>`}
4> HeapMetrics{area=PS Eden Space, used=19258296, max=1409286144, ratio=0.013665284429277693, jobId=3, hostname=`<your_hostname>`}
```

You can see a prefix added in the output lines: `3>` and `4>`. This is a feature of the `DataStream.print()` function. The prefix refers to the sequential id of each parallel instance of the sink.

Occasionally, the application triggers alerts, based on the alerting logic, that are printed to stderr through the logging framework.

```
08:20:11,829 INFO  com.cloudera.streaming.examples.flink.LogSink  - HeapAlert{message='42 was found in the HeapMetrics ratio.', triggeringStats=HeapMetrics{area=PS Eden Space, used=23876376, max=1409286144, ratio=0.016942177500043596, jobId=0, hostname=`<your_hostname>`}}
```

Let's explore this logging implementation and it's configuration.

## Logging framework

`LogSink` is a custom sink implementation that simply sends the messages to the logging framework. The logs can be redirected through log4j to any centralized logging system, or simply printed to the standard output in case of debugging. The quick start application provides a sample log4j configuration for redirecting the alert logs to the standard error.

```
log4j.rootLogger=INFO, stdout

log4j.logger.com.cloudera.streaming.examples.flink.LogSink=INFO, stderr
log4j.additivity.com.cloudera.streaming.examples.flink.LogSink=false

log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
log4j.appender.stdout.Target   = System.out
log4j.appender.stdout.layout.ConversionPattern=%d{HH:mm:ss,SSS} %-5p %-60c %x - %m%n

log4j.appender.stderr=org.apache.log4j.ConsoleAppender
log4j.appender.stderr.layout=org.apache.log4j.PatternLayout
log4j.appender.stderr.Target   = System.err
log4j.appender.stderr.layout.ConversionPattern=%d{HH:mm:ss,SSS} %-5p %-60c %x - %m%n
```
You can configure the alert mask with a more simple value to produce more frequent hits for testing:

```
--alertMask 42
--alertMask 4
```

## Running the application on a Cloudera cluster
The Simple Flink Application Tutorial can be deployed on a Cloudera Runtime cluster remotely. The actual version of the application was tested on Cloudera Runtime 7.0.3.0 and FLINK-1.9.1-csa1.1.0.0-cdh7.0.3.0-79-1753674 without any security integration on it. 

After you have [built](#Build) the project, run the application from a Flink GateWay node using the following command:

**Note**
Don't forget to [set up your HDFS home directory](https://docs.cloudera.com/csa/1.2.0/installation/topics/csa-hdfs-home-install.html).

```
flink run --jobmanager yarn-cluster --detached --parallelism 2 --yarnname HeapMonitor target/flink-simple-tutorial-1.2-SNAPSHOT.jar
```

After launching the application, Flink creates a YARN session and launches a dashboard where the application can be monitored. The Flink dashboard can be reached from Cloudera Manager with the following path: 
`Cluster->Yarn->Applications->application_<ID>->Tracking URL:ApplicationMaster`.

![YarnApp](images/YarnApp.png "The Flink application web dashboard is accessible from YARN.")

Following through this link, you can access the Flink application dashboard. On the dashboard, you can choose the Task Managers tab on the left navigation pane to gain access to the logs.

![TaskLogs](images/TaskLogs.png "The logs are accessible on the TaskManager pane of the web dashboard.")

In this case, we have actually run the application with the default `log4j.configuration` controlled by Cloudera Manager, and not with the one we previously used in the IDE locally.

### Writing logs to Kafka
Log messages from a Flink application can also be collected and forwarded to a Kafka topic for convenience. This requires only a few extra configuration steps and dependencies in Flink. The default log4j configuration can be overridden with the following command parameter:

```
--yarnconf log4j.configuration.file=kafka-appender/log4j.properties
```

You will need to edit the `kafka-appender/log4j.properties` file and replace the placeholders in the line below with the details of your Kafka brokers:

```
log4j.appender.kafka.brokerList=<your_broker_1>:9092,<your_broker_2>:9092,<your_broker_3>:9092
```

By default, we are using the `flink-heap-alerts` Kafka topic in the application for tracking the alerts. You can create this topic in your application as follows:
```
kafka-topics --create --partitions 16 --replication-factor 1 --zookeeper $(hostname -f):2181/kafka --topic flink-heap-alerts
```
**Note** 
In the above command `$(hostname -f)` assumes that you are running Zookeeper on the Flink Gateway node. If you are running it on a different node, simply replace it with your Zookeeper hostname.

Here is an example for the full command with Kafka logging:
```
flink run --jobmanager yarn-cluster \
          --yarnconf log4j.configuration.file=kafka-appender/log4j.properties \
          --detached  \
          --parallelism 2 \
          --yarnname HeapMonitor \
          target/flink-simple-tutorial-1.2-SNAPSHOT.jar
```
**Note**
In the CSA 1.1.0.0 release the `org.apache.kafka.log4jappender.KafkaLog4jAppender` class is not present on the TaskManagers' classpath. As a workaround, we referenced the `com.cloudera.kafka.log4jappender.KafkaLog4jAppender` class in the [log4j.properties](kafka-appender/log4j.properties) file that is shipped with CDP Data Center.

As referencing a custom jar in Flink is a common use case, it is worth mentioning that alternatively, `--yarnship /opt/cloudera/parcels/CDH/jars/kafka-log4j-appender-*.jar` could be appended to shade the Apache version of kafka-log4j-appender with the Cloudera version on the classpath of each TaskManagers.

Then, accessing the logs from the Kafka topic will look like this:
```
kafka-console-consumer --bootstrap-server <your_broker>:9092 --topic flink-heap-alerts
...
00:17:53,398 INFO  com.cloudera.streaming.examples.flink.LogSink                 - HeapAlert{message='42 was found in the HeapMetrics ratio.', triggeringStats=HeapMetrics{area=PS Eden Space, used=54560840, max=94371840, ratio=0.578147464328342, jobId=0, hostname='<yourhostname>'}}

00:17:53,498 INFO  com.cloudera.streaming.examples.flink.LogSink                 - HeapAlert{message='42 was found in the HeapMetrics ratio.', triggeringStats=HeapMetrics{area=PS Eden Space, used=54560840, max=94371840, ratio=0.578147464328342, jobId=0, hostname='<yourhostname>'}}

00:17:53,599 INFO  com.cloudera.streaming.examples.flink.LogSink                 - HeapAlert{message='42 was found in the HeapMetrics ratio.', triggeringStats=HeapMetrics{area=PS Eden Space, used=54560840, max=94371840, ratio=0.578147464328342, jobId=0, hostname='<yourhostname>'}}

00:17:53,700 INFO  com.cloudera.streaming.examples.flink.LogSink                 - HeapAlert{message='42 was found in the HeapMetrics ratio.', triggeringStats=HeapMetrics{area=PS Eden Space, used=54560840, max=94371840, ratio=0.578147464328342, jobId=0, hostname='<yourhostname>'}}

00:17:53,800 INFO  com.cloudera.streaming.examples.flink.LogSink                 - HeapAlert{message='42 was found in the HeapMetrics ratio.', triggeringStats=HeapMetrics{area=PS Eden Space, used=54560840, max=94371840, ratio=0.578147464328342, jobId=0, hostname='<yourhostname>'}}
```
### Writing output to HDFS

On a cluster environment, it is more preferable to write the output to a durable storage medium, that is why we chose HDFS for this storage layer. You can switch to the HDFS writer from the stdout writer with the following parameter:

```
--cluster true
```

By default, the output files will be stored under `hdfs:///tmp/flink-heap-stats`, but the output location is configurable with the `--output` parameter. The complete command that includes saving the output to HDFS and logging to Kafka looks like this:

```
flink run --jobmanager yarn-cluster \
          --yarnconf log4j.configuration.file=kafka-appender/log4j.properties \
          --detached  \
          --parallelism 2 \
          --yarnname HeapMonitor \
          --cluster true \
           target/flink-simple-tutorial-1.2-SNAPSHOT.jar
```

To inspect the output, you can call `hdfs` directly:

```
hdfs dfs -cat /tmp/flink-heap-stats/*/*
...
HeapMetrics{area=PS Eden Space, used=50399064, max=90701824, ratio=0.5556565654071081, jobId=1, hostname='<yourhostname>'}
HeapMetrics{area=PS Survivor Space, used=903448, max=15728640, ratio=0.05743967692057292, jobId=1, hostname='<yourhostname>'}
HeapMetrics{area=PS Old Gen, used=19907144, max=251658240, ratio=0.07910388310750326, jobId=1, hostname='<yourhostname>'}
```
