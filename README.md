# Reference Architecture for Flink on Cloudera

This repo contains reference Flink Streaming applications for a few example use-cases. These examples should serve as good starting points when building production grade streaming applications as they come with detailed development, configuration and deployment guidelines.

## Stateless Monitoring Application

[This application](flink-quickstart-cdh) demonstrates some basic capabilities of the DataStream API to build a simple JVM heap monitor pipeline with alerting capabilities. It shows the essentials of simple flink applications together with some useful tricks to set up a robust logging configuration using Kafka.

## Stateful Transaction and Query Processor Service

[This application](flink-quickstart-cdh-state) implements a production grade stateful application for handling incoming item transactions together with some querying capabilities.

We dive deeper into structuring streaming application code, state handling and resource configuration. We also show how to set up our Kafka data sources and sink for scalability and how to properly validate our pipeline before deployment.
