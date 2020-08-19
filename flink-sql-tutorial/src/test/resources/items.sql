USE CATALOG hive;

CREATE DATABASE itemdb;
USE itemdb;

DROP TABLE IF EXISTS ItemTransactions;
DROP TABLE IF EXISTS WindowedQuantity;

CREATE TABLE ItemTransactions (
	transactionId BIGINT,
	ts            BIGINT,
	itemId        STRING,
	quantity      INT,
	event_time AS CAST(from_unixtime(floor(ts/1000)) AS TIMESTAMP(3)),
	WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
	'connector.type'                         = 'kafka',
	'connector.version'                      = 'universal',
	'connector.topic'                        = 'transaction.log.1',
	'connector.startup-mode'                 = 'earliest-offset',
	'connector.properties.group.id'          = 'flink-sql-example',
	'connector.properties.bootstrap.servers' = '<your_broker>:9092',
	'format.type'                            = 'json'
);

CREATE TABLE WindowedQuantity (
	window_start TIMESTAMP(3),
	itemId       STRING,
	volume       INT
) WITH (
	'connector.type'                         = 'kafka',
	'connector.version'                      = 'universal',
	'connector.topic'                        = 'transaction.output.log.1',
	'connector.properties.bootstrap.servers' = '<your_broker>:9092',
	'format.type'                            = 'json'
);

INSERT INTO WindowedQuantity
SELECT TUMBLE_START(event_time, INTERVAL '10' SECOND) AS window_start, itemId, sum(quantity) AS volume
FROM ItemTransactions
GROUP BY itemId, TUMBLE(event_time, INTERVAL '10' SECOND);
