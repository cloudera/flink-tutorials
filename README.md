# Reference Architecture for Flink on CDH

## Introduction

The goal of this document is to give architects and software engineers enough context and practical guidelines to start building low-latency, stateful streaming applications that run on the CDH platform using Flink.

## Dataflow programming model

A full introduction to the programming model can be found on the [official flink website](https://ci.apache.org/projects/flink/flink-docs-release-1.8/concepts/programming-model.html). What follows here is a quick overview of the core concepts.

### Streams, operators and dataflows

The basic building blocks of Flink programs are streams and transformations.

**Data Stream** :  A flow of data records. Can be finite (file, db table, etc.) or infinite (message queues, socket connections, etc.) Streams are usually distributed (parallel) and can be partitioned by the user to achieve certain application semantics during processing. Data streams can only be created from input sources or as a result of a transformation on another datastream.

**Transformations** : A transformation is an operation applied to one more streams and produces one or more output streams as a result. These operators generally process the incoming records one-by-one for maximal flexibility and minimal latency but optimised APIs exist for sophisticated window and batch processing.

Flink programs are streaming dataflows that consist of data streams and transformations. We can represent these dataflows as directed acyclic graphs (DAGs) where data records flow from the sources, through the operators to the data sinks.

**TODO: Dataflow pic**

***Important to know***

Flink streaming programs are always executed lazily. Developers compose the full dataflow pipeline from streams and transformations and computation is only started when `StreamExecutionEnvironment.execute(...)` method is called. This allows the system to perform optimizations on the pipeline.


### Stateful applications

...

### Working with time and windows

...

### Processing guarantees

Flink supports strong consistency guarantees for stateful streaming applications. While it is possible to select weaker processing guarantees for minimal performance gain, we always recommend that users select `EXACTLY-ONCE` processing semantics.

With this setting Flink will take periodical checkpoints of the processing state (at pre-configured intervals) that are used for recovery in case of a failure.

When configured properly these checkpoints are lightweight and do not have a significant impact on the application performance.

...

## Flink streaming on the CDH platform

To understand how Flink fits together with other systems in the CDH platform we need to look at how streaming applications are structured and how different components interact with the rest of the data stack.

Basically every stream processing application consists of three high level components:

 1. Data Sources
 2. Stream processing logic / flow
 3. Data Sinks

We will look at these components in detail now.

### Data Sources

Flink provides a very simple interface to implement data sources. It also comes bundled with battle tested connectors to our favourite systems.

#### Kafka source connector

[Official Documentation](https://ci.apache.org/projects/flink/flink-docs-release-1.8/dev/connectors/kafka.html#kafka-consumer)

Apache Kafka is a massively scalable, high performance message queue. It is the most common, and probably the most robust ,input and output source (data transport layer) for production streaming applications.

Flink comes with a Kafka source connector that fully integrates with the underlying checkpointing mechanism to provide scalability and exactly-once consumer semantics. Flink tracks the offsets of consumed messages ensuring that every message is processed by the application exactly once.

**Key features of the connector:**
 - Supports arbitrary data formats: Avro, Json, Schema registry, Custom types
 - Specify read position when starting the application the first time: earliest, latest, group-offset, custom-offset
 - Topic and partition discovery through regex pattern
 - Integrated event time and watermark extraction

**Configuring Kafka topics for production applications**

There are two key parameters that should be set carefully for every Kafka topic that will be used as an input source:

***Partition count:*** A kafka partition is the unit of parallelism for consumers. The number of partitions should be large enough so that readers can consume it at rates several times higher than the incoming data load. This is necessary to ensure fast recovery times.

**TODO: Some actual numbers and recommendations**

It is very important to set the number of partitions correctly before starting the application. Changing the number of partitions later can cause data consistency problems within the application.

***Retention policy:*** Flink relies on Kafka to store messages for error recovery. This leads to very good performance characteristics but also means that error recovery (without data loss) is only possible within the retention period.

Having a larger retention period also allows developers to easily test-run their applications for tuning purposes on live data.

We recommend at least one week of retention for critical application data.

**Developer tips**
 1. A single consumer can consume several topics at the same time
 2. All sources should have a unique name and uid
 3. To handle parsing and data errors use `Either` or `Optional` types and always implement error handling
 4. Most built-in formats don't support out-of-the-box error handling
 5. Consumer starting position is only respected when the application starts without state (the first time)

### TODO: What other sources

#### Custom sources implementations

TODO: simple guide to building a custom source

### Application logic

#### Interacting with external services

TODO: Stateful vs stateless applications, external calls, sync async.

### Data Sinks

#### Kafka sink connector

#### Streaming file sink

### State backends

TODO: State backend types when to use them and basic config guidelines

#### FS state backend

#### RocksDB state backend

### Logging, monitoring and metrics

TODO: Where do logs and metrics go, how to find what we are looking for + how to improve: Kafka logger (+metrics reported?)

## Running Flink on Yarn

TODO: How Flink applications run on YARN, types of containes and overview of the deployment flow.

### Configuring YARN for Flink applications

### Resource allocation guidelines

## Development guidelines

TODO: How to structure code, build jars etc.

### Guidelines for testing streaming pipelines

TODO: basic tips for testing pipelines, unit vs integration testing streaming apps
