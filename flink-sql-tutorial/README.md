# Flink SQL Tutorial

## Table of contents
1. [Introduction](#introduction)
2. [SQL Client Configuration](#sql-client-configuration)
    + [Configuring the SQL client for session mode](#configuring-the-sql-client-for-session-mode)
3. [Using Flink SQL to query streams](#using-flink-sql-to-query-streams)
    + [Generating input data](#generating-input-data)
    + [Streams and Tables](#streams-and-tables)
    + [Basic Flink SQL queries](#basic-flink-sql-queries)
    + [Handling ItemTransactions and Queries](#handling-itemtransactions-and-queries)
4. [Executing SQL from Java](#executing-sql-from-jsava)

## Introduction

This tutorial builds on the [stateful-tutorial](../flink-stateful-tutorial) and re-uses concepts and data generators for simplicity. Before you proceed here please read the introductory section of the stateful tutorial to familiarise yourself with the Item Warehouse use case.

## SQL Client Configuration

The Flink SQL client behaviour is controlled by 3 layers of configuration.

  1. Regular Flink configuration (`flink-conf.yaml`)
  2. SQL Client Default settings (`sql-client-defaults.yaml`)
  3. Custom environment settings

The regular Flink configuration is same as with all other Flink jobs and we can simply configure it through the Cloudera manager.

The SQL Client default settings file located by default under `/etc/flink/conf/sql-client-defaults.yaml` stores the default SQL specific settings such as Catalog configurations. Some of these configs are also exposed in the Cloudera manager.

Users have the option to create a custom environment yaml file that can override some of these settings and pass it when starting the SQL client with the `-e sql-env.yaml` parameter. This will come in handy once we want to define our own UDFs and control lower level execution parameters.

### Configuring the SQL client for session mode

Every Flink SQL query is an independent Flink job. As with other Flink applications we have to decide on how we want run them. They can run as standalone YARN applications (default mode for all Flink jobs) or we can run them on a Flink session cluster.

While the standalone (per-job) mode gives better resource isolation and production characteristics, it comes with much higher
startup times that can be annoying when exploring the SQL API.

If we decide to stay in the per-job mode we don't have to do anything just start the sql client without any additional parameters.

If we decide to run on a Flink Session cluster we need to do 2 things:
 1. Start a Flink Session
 2. Configure the SQL client for session mode

Starting a Flink YARN session is very easy from the command line:
```
flink-yarn-session -tm 2048 -s 2 -d
```
This starts a new session and specifies the taskmanager sizes for future executors. We don't have to set the size of our cluster, because Flink will do this automatically for us. The cluster will start without any TaskManager containers and will grow and shrink in size as the number of deployed queries changes.

To configure our SQL client for session mode we need to create an environment yaml file (`sql-env.yaml`):

```
configuration:
  execution.target: yarn-session
```

This will simply override the Flink configuration for the execution target to yarn-session mode. Our start command will look like this:

```
flink-sql-client embedded -e sql-env.yaml
```

## Using Flink SQL to query streams

### Generating input data

Our Flink SQL queries will run against the same Kafka topics as our `ItemTransactionJob` would, so make sure that the `KafkaDataGeneratorJob` is running in the background with `generate.queries=true` parameter set to generate a stream of item queries as well.

Later parts of this tutorial assume that the transactions and queries are generated with the following generator configs:

```
transaction.input.topic=transaction.log.1
query.input.topic=query.input.log.1

generate.queries=true
```

Detailed instructions for the generator can be found [here](../flink-stateful-tutorial#kafka-data-generator).

### Streams and Tables

Now with the configs and input data sorted, we can go ahead and start the client

```
flink-sql-client embedded (-e sql-env.yaml)
```

If everything is set up correctly we will get a SQL prompt where we can execute a simple `SHOW TABLES;` statement to test how commands work.

Before we can dive into writing and running SQL queries, we have to define our source tables corresponding to our `ItemTransactions`.
We will expose all the json fields as columns with their respective data types and we define an extra `event_time` column derived from the epoch ts with the corresponding WATERMARK definition. The WATERMARK definition is necessary for event time windowed processing as we will see later.

The connector properties are encapsulated in the WITH clause where we can change Kafka connection and topic parameters and also the start-offset behaviour.

When copying the below commands, please keep in mind that the SQL CLI can only execute one SQL statement at a time otherwise you will get an error.

```
CREATE TABLE ItemTransactions (
	transactionId    BIGINT,
	ts    BIGINT,
	itemId    STRING,
	quantity INT,
	event_time AS CAST(from_unixtime(floor(ts/1000)) AS TIMESTAMP(3)),
	WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
	'connector.type'    	 = 'kafka',
	'connector.version' 	 = 'universal',
	'connector.topic'   	 = 'transaction.log.1',
	'connector.startup-mode' = 'earliest-offset',
	'connector.properties.bootstrap.servers' = '<broker_address>',
	'format.type' = 'json'
);
```

Once we have created our tables, a simple `SHOW TABLES;` should list them for us.

### Basic Flink SQL queries

Let's try the simplest SELECT query possible to look at our input data and validate that everything works correctly:

```
> SELECT * FROM ItemTransactions;

transactionId                        ts                    itemId                  quantity                event_time
---------------------------------------------------------------------------------------------------------------------------
8481196166629043825             1582541568729                    item_1                       -45       2020-02-24T02:52:48
5350938227496695865             1582541568671                    item_2                       -35       2020-02-24T02:52:48
1830974069463357363             1582541568829                    item_1                        35       2020-02-24T02:52:48
650615302024913829              1582541568771                    item_1                        65       2020-02-24T02:52:48
```

New records are continuously displayed until we hit `Q` to exit the query and shut down the Flink job.

Now that we have everything set up and working correctly it's time to experiment with some more interesting queries.

**Simple window aggregate of total transaction volume by item**
```
SELECT TUMBLE_START(event_time, INTERVAL '10' SECOND) as window_start, itemId, sum(quantity) as volume
FROM ItemTransactions
GROUP BY itemId, TUMBLE(event_time, INTERVAL '10' SECOND);
```

If we want to write the output of our window aggregate query to Kafka, we need to define a new table and use `INSERT INTO`:

```
CREATE TABLE WindowedQuantity (
	window_start    TIMESTAMP(3),
	itemId    STRING,
	volume INT
) WITH (
	'connector.type'    	 = 'kafka',
	'connector.version' 	 = 'universal',
	'connector.topic'   	 = 'transaction.output.log',
	'connector.properties.bootstrap.servers' = '<broker_address>',
	'format.type' = 'json'
);

INSERT INTO WindowedQuantity
SELECT TUMBLE_START(event_time, INTERVAL '10' SECOND) as window_start, itemId, sum(quantity) as volume
FROM ItemTransactions
GROUP BY itemId, TUMBLE(event_time, INTERVAL '10' SECOND);
```

Please note that `INSERT INTO` queries always run in detached mode so we won't be able to see the output directly on the console. After running it:

```
[INFO] Table update statement has been successfully submitted to the cluster:
Job ID: 44261a9bccf467a289742a1d99c21dda
```

However we can simply run a select query on the table we are inserting into: `SELECT * FROM WindowedQuantity;`

```
> SELECT * FROM WindowedQuantity;

window_start                           itemId                    volume
------------------------------------------------------------------------
2020-03-05T06:44:20                    item_1                       740
2020-03-05T06:44:20                    item_3                       265
2020-03-05T06:44:20                    item_5                       125
2020-03-05T06:44:20                    item_4                        15
2020-03-05T06:44:20                    item_2                       805
```


Finding Top-3 items with the most number of transactions in every 10 minute window.
More info on top-n query syntax: https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/table/sql/queries.html#top-n

```
SELECT * FROM (
  SELECT * ,
  ROW_NUMBER() OVER (
    PARTITION BY window_start
    ORDER BY num_transactions desc
  ) AS rownum
  FROM (
    SELECT TUMBLE_START(event_time, INTERVAL '10' MINUTE) AS window_start, itemId, COUNT(*) AS num_transactions
    FROM ItemTransactions
    GROUP BY itemId, TUMBLE(event_time, INTERVAL '10' MINUTE)
  )
)
WHERE rownum <=3;
```

### Handling ItemTransactions and Queries

Let's now look at how we can implement our original ItemTransaction logic in SQL context where we assume that there is a web server sending item
queries. Once again we want to be able to return a stream of query results for the users/backend.

We start by defining a new table for the item queries and their future results:

```
CREATE TABLE Queries (
	queryId    BIGINT,
	ts    BIGINT,
	itemId    STRING,
	event_time AS CAST(from_unixtime(floor(ts/1000)) AS TIMESTAMP(3)),
	WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
	'connector.type'    	 = 'kafka',
	'connector.version' 	 = 'universal',
	'connector.topic'   	 = 'query.input.log.1',
	'connector.startup-mode' = 'earliest-offset',
	'connector.properties.bootstrap.servers' = '<broker_address>',
	'format.type' = 'json'
);
```

In order to calculate the result of the item query we have to join the `Queries` and `ItemTransactions` on the `itemId` column.
For every incoming query we now will return the total transaction quantity in the last minute.

```
SELECT queryId, q.event_time as query_time, t.itemId, sum(t.quantity) AS recent_transactions
FROM
  ItemTransactions AS t,
  Queries AS q
WHERE
  t.itemId = q.itemId AND
  t.event_time BETWEEN q.event_time - INTERVAL '1' MINUTE AND q.event_time
GROUP BY
  t.itemId, q.event_time, q.queryId;
```

As we group on `event_time` of the queries (and not a window) Flink treats this as a retract Table where updated results can overwrite previous outputs.
This means that we can only write it to Table sinks that support retractions.

As Kafka only supports appends we have to transform our query a little to window the outputs.

The otutput table:

```
CREATE TABLE QueryResult (
  queryId    BIGINT,
	query_time    TIMESTAMP(3),
	itemId    STRING,
	quantity INT
) WITH (
	'connector.type'    	 = 'kafka',
	'connector.version' 	 = 'universal',
	'connector.topic'   	 = 'query.output.log',
	'connector.properties.bootstrap.servers' = '<broker_address>',
	'format.type' = 'json'
);
```

The actual output query:
```
INSERT INTO QueryResult
SELECT
  q.queryId,
  TUMBLE_START(q.event_time, INTERVAL '1' SECOND) as query_time,
  t.itemId,
  sum(t.quantity) AS quantity
FROM
  ItemTransactions AS t,
  Queries AS q
WHERE
  t.itemId = q.itemId AND
  t.event_time BETWEEN q.event_time - INTERVAL '1' MINUTE AND q.event_time
GROUP BY
  t.itemId, q.queryId, TUMBLE(q.event_time, INTERVAL '1' SECOND);
```

Once the job started we can simply query the output table to see the results:

```
> SELECT * from QueryResult;

queryId                         query_time                       itemId                  quantity
-------------------------------------------------------------------------------------------------
8803696783849659441       2020-03-03T03:23:57                    item_6                       100
3913845414840092492       2020-03-03T03:23:58                    item_5                       170
8139675561435722961       2020-03-03T03:23:58                    item_3                       530
3106569172855270754       2020-03-03T03:23:59                    item_4                       400
```

## Executing SQL from Java

To run the SQL queries in production our recommendation is to package them into a standard Java Flink program.
We are providing minimalistic utility which takes an SQL script and executes it on the cluster.
This utility also showcases using the Hive catalog.

You can follow these steps to run the enclosed example query:

```
mvn clean install
flink run --classpath file:///opt/cloudera/parcels/CDH/lib/hadoop/client/hadoop-mapreduce-client-core.jar --classpath file:///opt/cloudera/parcels/CDH/lib/hive/lib/libfb303-0.9.3.jar --classpath file:///opt/cloudera/parcels/CDH/lib/hive/lib/hive-exec.jar -d -p 2 -ys 2 -ynm HiveSqlExecutor -c com.cloudera.streaming.examples.flink.SqlScriptExecutor target/flink-sql-tutorial-1.2-SNAPSHOT.jar src/main/resources/items.sql
```
