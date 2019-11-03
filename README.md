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

## Cluster setup

The examples were tested on a CDH6.3 and CDP7.0 clusters with Flink 1.9. 

To provision a sufficient CDH6.3 cluster you can use the [Cluster-Setup Jenkins job](https://master-01.jenkins.cloudera.com/job/Cluster-Setup/build?delay=0sec) with the following configurations:
1. CM_VERSION: `cm6.3.0`
2. CDH: `cdh6.3.0`
3. OPTIONAL_ARGS: `-is=ZOOKEEPER,HDFS,YARN,KAFKA`

To provision a sufficient CDPD7.0 cluster you can use the [Cluster-Setup-cdpd Jenkins job](https://master-01.jenkins.cloudera.com/job/Cluster-Setup-cdpd/build?delay=0sec) with the following configurations:
1. OPTIONAL_ARGS: `-is=ZOOKEEPER,HDFS,YARN,KAFKA`

To provision a secured CDH6.3 or CDP7.0 environment the following additional options should be set:
1. SSL: true
2. TLS: true
3. KERBEROS: KERBEROS(MIT) OR AD KERBEROS

Once your Jenkins job is finished please follow this [guide](https://cloudera.atlassian.net/wiki/spaces/ENG/pages/143427201/Flink) to add the Flink service. We will add a specific Jenkins job to fully automate this process. 
