# Heap monitor application

## Problem description

We are going to write a real-time application that will monitor Java heap usage and can trigger alerts when critical memory consumption levels are reached.

The memory information tracked in this application should be written to HDFS for further analysis and alerts should be logged to the destination of our choice for further action.

...

## Building our application

Every Flink application is built from 4 main components:

1. **Application entry class:** Defines the `StreamExecutionEnvironment` and creates the pipeline
2. **Data Sources:** Access the heap information and make it available for processing
3. **Processing operators and flow*:** Process the heap usage information to detect critical memory levels and produce the alerts
4. **Data Sinks:** Store the memory information collected on HDFS and log the alerts to the configured logger

### Application main class

Every Flink application has to define a main class that will be executed on the client side on job submission. The main class will define the application pipeline that is going to be executed on the cluster.

Our main class is the `HeapMonitorPipeline` which contains a main method like any standard Java application. The arguments passed to our main method will be determined by us when we use the flink-client. We use the  `ParameterTool` utility to conveniently pass parameters to our job that we can use in our operator implementations.

The first thing we do is create the `StreamExecutionEnvironment` which can be used to define DataStreams and data processing logic as we will se below. It is also used to configure important job parameters such as checkpointing behaviour to guarantee data consistency for our application.


```
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.enableCheckpointing(10_000);
```

The `getExecutionEnvironment()` static call guarantees that our pipeline will always be using the correct environment where it is executed. When running from our IDE this means a local execution environment, and when running from the client for cluster submission it will return the yarn execution environment. This ensures that our pipeline can be executed both locally for testing purposes and for cluster deployment without modifying our pipeline.

Even though this application doesn't rely on user defined state we enable checkpointing every 10 seconds to allow the datasinks to produce consistent output to HDFS.

The rest of the main class defines the application sources, processing flow and the sinks followed by the `execute()` call which will trigger the actual execution of the pipeline either locally or on the cluster.

### Creating the stream of heap information

The key data abstraction for every Flink streaming application is the `DataStream` which is a bounded or unbounded flow of records. In our application we will be processing memory related information so we created the `HeapStats` class to represent our data records.

The `HeapStats` class has a few key properties that make it efficiently serializable by the Flink type system that we must point out here:

1. It is public and standalone class (no non-static inner class)
2. It has a public empty constructor
3. All fields are public non final

It is possible to structure the class differently by keeping the same serialization properties, for the exact rules please refer to the docs: https://ci.apache.org/projects/flink/flink-docs-stable/dev/types_serialization.html#rules-for-pojo-types

Now that we have our record class we need to produce a `DataStream<HeapStats>` of the heap information, which can be done by adding a data source in our application. The `HeapMonitorSource` class extends the `RichParallelSourceFunction<HeapStats>` abstract class which allows us to use it as a data source.

Let's take a closer look at this class:

- Every Flink source must implement the `SourceFunction` interface which at it's core provides 2 methods that will be called by the Flink runtime during cluster execution:
 - `run(SourceContext)`: This method should contain the data producer loop. Once it finishes the source shuts down.
 - `cancel()`: This method is called if the source should terminate before it is finished, i.e. break out early from the `run` method

- The `RichParallelSourceFunction` extends the basic `SourceFunction` behaviour in 2 important ways:
 - It extends the `ParallelSourceFunction`, allowing Flink to create multiple instances of the source logic. One per parallel task instance.
 - It extends the `RichFunction` abstract class which allows the implementation to access runtime information such as parallelism and subtask index that we will leverage in our source implementation

Our source will continuously poll the heap memory usage of this application and output it along with some task related information producing the datastream.

### Computing GC warnings and heap alerts

The core data processing logic is encapsulated in the `HeapMonitorPipeline.computeHeapAlerts(DataStream<HeapStats> statsInput, ParameterTool params)` method that takes as input the DataStream of heap information and should produce a datastream of alerts when the conditions are met.

The reason for structuring the code this way is to make our pipeline easily testable later by replacing our production data source with the test data stream.

The core alerting logic is implemented in the `AlertingFunction` class. It is a `FlatMapFunction` that filters out incoming heap stats objects according to the configured thresholds and converts them to `HeapAlerts`. We levarage the `ParameterTool` object coming from our main progrem entry point to make these alerting thresholds configurable when using the flink client later.

### Producing some actual output

TODO:

## Testing our data pipeline

We will write a simple JUnit test to verify our core application logic. The test is implemented in the `HeapMonitorPipelineTest` and should be regarded as an integration test of the application flow. Even though this pipeline is very simple we can later use the same idea to test more complex application flows.

Our test mimics our application main class with only minor differences:
1. We create the StreamExecutionEnvironment the same ways
2. Instead of using our source implementation we will use the `env.fromElements(..)` method to pre-populate a DataStream with some testing data.
3. We feed this data to our static data processing logic like before
4. Instead of writing the output anywhere we verify the correctness once the pipeline finished.

### Producing test input

The Flink environment has several methods that can be used to produce data for testing. In our case we pass the elements of the stream directly, but we could have also implemented a custom source for example. We have to keep in mind that the ordering guarantees of the messages processed depend on the sources and partitioning of the downstream flow.

### Collecting the test output

To be able to assert the correctness of the output, first we need to get hold of the output elements. The simplest thing to do in this case was to write a custom data sink that collects the incoming elements into a static `Set<HeapAlert>`.

We have specifically set the parallelism of our data sink to 1 to avoid any concurrency issues that might arise from parallel execution.

As we cannot always force strict ordering for the output elements we used a `Set` instead of a `List` to compare expected output regardless of the order. This might or might not be the correct approach depending on the application flow, but it works very well in our case.


## Running the application on YARN

...
