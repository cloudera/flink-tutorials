# Reference Architecture for Flink on Cloudera

This repo contains reference Flink Streaming applications for a few example use-cases. These examples should serve as good starting points when building production grade streaming applications as they come with detailed development, configuration and deployment guidelines.

## Stateless Monitoring Application

[This application](flink-simple-quickstart) demonstrates some basic capabilities of the DataStream API to build a simple JVM heap monitor pipeline with alerting capabilities. It shows the essentials of simple flink applications together with some useful tricks to set up a robust logging configuration using Kafka.

## Stateful Transaction and Query Processor Service

[This application](flink-stateful-quickstart) implements a production grade stateful service for handling incoming item transactions together with some querying capabilities.

We dive deeper into structuring streaming application code, state handling and resource configuration. We also show how to set up our Kafka data sources and sink for scalability and how to properly validate our pipeline before deployment.

## Cluster setup

The examples were tested on a CDH 6.3 cluster with Flink 1.9. To provision a sufficient CloudCat cluster you can use [this](https://master-01.jenkins.cloudera.com/job/Cluster-Setup/build?delay=0sec) Jenkins job with the following configurations:
1. CM_VERSION: `cm6.3.0`
2. CDH: `cdh6.3.0`
3. OPTIONAL_ARGS: `-is=ZOOKEEPER,HDFS,YARN,KAFKA`

Once your Jenkins job is finished please follow this [guide](https://cloudera.atlassian.net/wiki/spaces/ENG/pages/143427201/Flink) to add the Flink service. We will add a specific Jenkins job to fully automate this process. 
