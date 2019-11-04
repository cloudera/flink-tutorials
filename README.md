# Tutorials for Flink on Cloudera

This repo contains reference Flink Streaming applications for a few example use-cases. These examples should serve as solid starting points when building production grade streaming applications as they include detailed development, configuration and deployment guidelines.
We suggest to refer to them in the following order even though they are designed to be standalone references:

## Stateless Monitoring Application

[The (flink-simple-tutorial)](flink-simple-tutorial) application demonstrates some basic capabilities of the DataStream API to build a simple monitoring application with alerting capabilities. It displays the essentials of Flink applications alongside best practices for setting up a robust logging configuration using Kafka.

## Stateful Transaction and Query Processor Service

[The (flink-stateful-tutorial)](flink-stateful-tutorial) application implements a production grade stateful service for handling incoming item transactions, while also exposing query capabilities.

We dive deeper into structuring streaming application code, state handling and resource configuration. We also present a detailed set up of Kafka data sources and sinks for scalability and discuss validation of our pipeline before deployment.

##  Flink Security Showcase Application
[The (flink-secure-tutorial)](flink-secure-tutorial) application demonstrates Flink security's features for applications intended to run in secured CDH/CDP environments. It covers Kerberos authentication and TLS encryption for HDFS and Kafka connectors.

