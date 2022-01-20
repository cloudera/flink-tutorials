# Tutorials for Flink on Cloudera

This repo contains reference Flink Streaming applications for a few example use-cases. These examples should serve as solid starting points when building production grade streaming applications as they include detailed development, configuration and deployment guidelines.

We suggest to refer the tutorials in the following order even though they are designed to be standalone references:


## Stateless Monitoring Application

The [flink-simple-tutorial](flink-simple-tutorial) application demonstrates some basic capabilities of the DataStream API to build a simple monitoring application with alerting capabilities. It displays the essentials of Flink applications alongside best practices for setting up a robust logging configuration using Kafka.


## Stateful Transaction and Query Processor Service

The [flink-stateful-tutorial](flink-stateful-tutorial) application implements a production grade stateful service for handling incoming item transactions, while also exposing query capabilities.

We dive deeper into structuring streaming application code, state handling and resource configuration. We also present a detailed set up of Kafka data sources and sinks for scalability and discuss validation of our pipeline before deployment.


##  Flink Security Showcase Application

The [flink-secure-tutorial](flink-secure-tutorial) application demonstrates Flink's security features for applications intended to run in secured CDP environments. It covers Kerberos authentication and TLS encryption for HDFS and Kafka connectors.


##  Flink SQL Showcase

The [flink-sql-tutorial](flink-sql-tutorial) demonstrates Flink's SQL features, via creating tables backed by Kafka topics. It builds on the datagenerator used in the [flink-stateful-tutorial](flink-stateful-tutorial) and showcases both the SQL cli and embedding Flink SQL statements into Java applications.


## Flink Quickstart Archetype

The [flink-quickstart-archetype](flink-quickstart-archetype) is a maven archetype for generating application skeletons specificly for Flink on Cloudera.


## Flink Quickstart Skeleton

The [flink-quickstart-skeleton](flink-quickstart-skeleton) is a maven project with the same content the archetype generates. This is for users who do not want or are unable to use the archetype.
