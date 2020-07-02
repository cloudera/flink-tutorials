# Flink Application Tutorial for Secured CDP Data Center Clusters

## Table of Contents
 1. [Overview](#overview)
 2. [Understanding Security Parameters](#understanding-security-parameters)
     + [application.properties](#applicationproperties)
 3. [Complete Command including Security](#complete-command-including-security)
     + [Preparation](#preparation)
     + [Steps to run the Secured Flink Quickstart Application](#steps-to-run-the-secured-flink-quickstart-application)
 4. [Kafka Metrics Reporter](#kafka-metrics-reporter)
 5. [Schema Registry Integration](#schema-registry-integration)
 6. [Sample commands](#sample-commands)
     + [Kerberos related commands](#kerberos-related-commands)
     + [Kafka related commands](#kafka-related-commands)

## Overview
This application demonstrates how to enable essential Flink security features for applications intended to run in secured CDP Data Center environments. For the sake of simplicity the application logic here is kept pretty basic: the application reads messages from a Kafka topic and stores them on HDFS.

```java
public class KafkaToHDFSSimpleJob {
 public static void main(String[] args) throws Exception {
  ...
  FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(P_KAFKA_TOPIC, new SimpleStringSchema(), properties);
  DataStream<String> source = env.addSource(consumer).name("Kafka Source");
  source.print();
  final StreamingFileSink<String> sink = StreamingFileSink
    .forRowFormat(new Path(P_FS_OUTPUT), new SimpleStringEncoder<String>("UTF-8"))
    .build();
  source.addSink(sink).name("FS Sink");
  env.execute("Flink Streaming Secured Job Sample");
  ...
 }
}
```
With this Secure Flink Application Tutorial, we are focusing on how to handle authentication and encryption (TLS) in Flink applications.

First, you need to clone and build the project:
```
> git clone https://github.com/cloudera/flink-tutorials.git
> cd flink-tutorials/flink-secure-tutorial
> mvn clean package
> cd target
```
On a **non-secured** CDP Data Center cluster, the command for the quick start application looks like this:

```
flink run -m yarn-cluster -d -p 2 \
flink-sec-tutorial-1.1-SNAPSHOT.jar \
--kafka.bootstrap.servers <hostname>:9093 \
--kafkaTopic flink \
--hdfsOutput hdfs:///tmp/flink-sec-tutorial
```
As you will later see, enabling security features make the command a bit more complicated, and thus we will show every needed step to create the secured code for the Flink application.

For information about Flink Security, see the [Security Overview](https://docs.cloudera.com/csa/1.1.0/security-overview/topics/csa-security.html) section in Cloudera Streaming Analytics document.

**Note**
For the sake of readability, the rest of the tutorial uses command line parameters in short form:
- Long form
`flink run --jobmanager yarn-cluster --detached --parallelism 2 --yarnname HeapMonitor
target/flink-simple-tutorial-1.1-SNAPSHOT.jar`

- Short form
`flink run -m yarn-cluster -d -p 2 -ynm HeapMonitor
target/flink-simple-tutorial-1.1-SNAPSHOT.jar`

**Note**
+Don't forget to [set up your HDFS home directory](https://docs.cloudera.com/csa/1.2.0/installation/topics/csa-hdfs-home-install.html)

## Understanding Security Parameters

Authentication related configurations are defined as Flink command line parameters (-yD):

```
-yD security.kerberos.login.keytab=test.keytab
-yD security.kerberos.login.principal=test
```

Internal network encryption related configurations are defined as Flink command line parameters (-yD):
```
-yD security.ssl.internal.enabled=true
-yD security.ssl.internal.keystore=keystore.jks
-yD security.ssl.internal.key-password=`cat pwd.txt`
-yD security.ssl.internal.keystore-password=`cat pwd.txt`
-yD security.ssl.internal.truststore=keystore.jks
-yD security.ssl.internal.truststore-password=`cat pwd.txt`
-yt keystore.jks
```

Kafka connector properties defined as normal job arguments, since there are no built-in configurations for them in Flink:

```
--kafka.security.protocol SASL_SSL \
--kafka.sasl.kerberos.service.name kafka \
--kafka.ssl.truststore.location /etc/cdep-ssl-conf/CA_STANDARD/truststore.jks
```

Any number of Kafka connector properties can be added to the command dynamically using the _"kafka."_ prefix. These properties are forwarded to the Kafka consumer after trimming the _"kafka."_ prefix from them:
```java
  Properties properties = new Properties();
        for (String key : params.getProperties().stringPropertyNames()) {
            if (key.startsWith(KAFKA_PREFIX)) {
                properties.setProperty(key.substring(KAFKA_PREFIX.length()), params.get(key));
            }
        }
  FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(P_KAFKA_TOPIC, new SimpleStringSchema(), properties);
```
**Note**
The trustore given for the Kafka connector is different from the one previously generated for Flink internal encryption. This is the truststore to access the TLS protected Kafka endpoint.

The keytab and the keystore files that are referred to as `-yD security.kerberos.login.keytab=test.keytab` and `-yt keystore.jks` respectively are distributed automatically to the temporary folder of the YARN container on the remote hosts. These properties are user specific, and usually cannot be added to the default Flink configuration unless a single technical user is used to submit the Flink jobs. If there is a dedicated technical user for submitting Flink jobs on a cluster, the keytab and keystore files can be provisioned to the YARN hosts in advance. This way the related configuration parameters can be set globally in Cloudera Manager using Safety Valves.

For more security information, see the Apache Flink documentation about [Kerberos](https://ci.apache.org/projects/flink/flink-docs-release-1.9/ops/security-kerberos.html), [TLS](https://ci.apache.org/projects/flink/flink-docs-release-1.9/ops/security-ssl.html) and [Kafka](https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/connectors/kafka.html#enabling-kerberos-authentication-for-versions-09-and-above-only) connector.


### application.properties
As you can see, the number of security related configuration options with various Flink connectors can get complicated really easily. Therefore, it is recommended to define as much security property as possible in a separate configuration file, and submit it along other business parameters.

```
flink run -m yarn-cluster -d -p 2 \
-yD security.kerberos.login.keytab=test.keytab \
-yD security.kerberos.login.principal=test \
-yD security.ssl.internal.enabled=true \
-yD security.ssl.internal.keystore=keystore.jks \
-yD security.ssl.internal.key-password=`cat pwd.txt` \
-yD security.ssl.internal.keystore-password=`cat pwd.txt` \
-yD security.ssl.internal.truststore=keystore.jks \
-yD security.ssl.internal.truststore-password=`cat pwd.txt` \
-yt keystore.jks \
-yt application.properties \
flink-sec-tutorial-1.1-SNAPSHOT.jar \
--properties.file application.properties
```

Flink has a built-in `ParameterTool` class to handle program arguments elegantly. You can merge the arguments given in the command line and configuration file like the example below:
```java
  ParameterTool params = ParameterTool.fromArgs(args);
  final String P_PROPERTIES_FILE = params.get("properties.file");

  if (P_PROPERTIES_FILE != null) {
    params = ParameterTool.fromPropertiesFile(P_PROPERTIES_FILE).mergeWith(params);
  }

  LOG.info("### Job parameters:");
  for (String key : params.getProperties().stringPropertyNames()) {
     LOG.info("Param: {}={}", key, params.get(key));
  }

  final String P_KAFKA_TOPIC = params.get("kafkaTopic");
  final String P_FS_OUTPUT = params.get("hdfsOutput");
```


## Complete Command including Security

### Preparation
* Cloudera Runtime 7.0+ clusters with Kerberos (MIT or AD) and TLS integration (Manual or Auto TLS) including services:
  * HDFS
  * Kafka
  * YARN
  * Flink
* Existing test user in the Kerberos realm and as local users on each cluster nodes. You can find some useful commands for fulfilling the prerequisites in a dedicated chapter [here](#sample-command).

### Steps to run the Secured Flink Application Tutorial

1. For Kerberos authentication, generate a keytab file for the user intended to submit the Flink job. Keytabs can be generated with `ktutil` as the following example:

  ```
  > ktutil
  ktutil: add_entry -password -p test -k 1 -e des3-cbc-sha1
  Password for test@:
  ktutil:  wkt test.keytab
  ktutil:  quit
  ```
2. For internal TLS encryption generate a keystore file for the user intended to run the Flink application. If the `JAVA_HOME` is not set globally on the host, then the keytool can be usually accessed at `/usr/java/default/bin/keytool`.

  ```
  keytool -genkeypair -alias flink.internal -keystore keystore.jks -dname "CN=flink.internal" -storepass `cat pwd.txt` -keyalg RSA -keysize 4096 -storetype PKCS12
  ```
  The key pair acts as the shared secret for internal security, and we can directly use it as keystore and truststore.

3. Using additional security configuration parameters, submit Flink application as normal:
  ```
  flink run -m yarn-cluster -d -p 2 \
  -yD security.kerberos.login.keytab=test.keytab \
  -yD security.kerberos.login.principal=test \
  -yD security.ssl.internal.enabled=true \
  -yD security.ssl.internal.keystore=keystore.jks \
  -yD security.ssl.internal.key-password=`cat pwd.txt` \
  -yD security.ssl.internal.keystore-password=`cat pwd.txt` \
  -yD security.ssl.internal.truststore=keystore.jks \
  -yD security.ssl.internal.truststore-password=`cat pwd.txt` \
  -yt keystore.jks \
  flink-sec-tutorial-1.1-SNAPSHOT.jar \
  --kafkaTopic flink \
  --hdfsOutput hdfs:///tmp/flink-sec-tutorial \
  --kafka.bootstrap.servers <hostname>:9093 \
  --kafka.security.protocol SASL_SSL \
  --kafka.sasl.kerberos.service.name kafka \
  --kafka.ssl.truststore.location /etc/cdep-ssl-conf/CA_STANDARD/truststore.jks
  ```
4. Send some messages to the _flink_ topic in Kafka. 
5. Check the application logs and the HDFS output folder to verify that the messages arrive as expected.

## Kafka Metrics Reporter
There is a metrics reporter implementation for writing Flink metrics to a target Kafka topic in JSON format. The corresponding security related Kafka properties can be set either in the command itself or globally using Cloudera Manager safety valve (Flink Client Advanced Configuration Snippet for flink-conf-xml/flink-cli-conf.xml).
```
-yD metrics.reporter.kafka.class=org.apache.flink.metrics.kafka.KafkaMetricsReporter \
-yD metrics.reporter.kafka.topic=metrics-topic.log \
-yD metrics.reporter.kafka.bootstrap.servers=morhidi-1.gce.cloudera.com:9093 \
-yD metrics.reporter.kafka.security.protocol=SASL_SSL \
-yD metrics.reporter.kafka.sasl.kerberos.service.name=kafka \
-yD metrics.reporter.kafka.ssl.truststore.location=/etc/cdep-ssl-conf/CA_STANDARD/truststore.jks \
```

## Schema Registry Integration

Flink Kafka sources and sinks can be used together with Cloudera Schema Registry to maintain schema information about your Kafka topics. In this example, the schema registry is used to provide automatic serialization/deserialization functionality for Kafka Avro messages.

The ```Message``` Avro type is a Java class generated from the Avro schema ```message.avsc``` during compile time:
```
{"namespace": "com.cloudera.streaming.examples.flink.data",
 "type": "record",
 "name": "Message",
 "fields": [
     {"name": "id", "type": "string"},
     {"name": "name",  "type": "string"},
     {"name": "description", "type": "string"}
 ]
}
```
The schema of the Avro types are automatically registered in the Cloudera Schema Registry at the first record serialization.

The schema registry can be plugged directly into a ```FlinkKafkaConsumer``` or a ```FlinkKafkaProducer``` using the appropriate ```ClouderaRegistryKafkaDeserializationSchema``` and ```ClouderaRegistryKafkaSerializationSchema``` classes respectively.

Two sample Jobs were written to demonstrate how to integrate with Cloudera Schema Registry from Flink in a secured way.

The ```AvroDataGeneratorJob``` uses a Kafka sink with ```ClouderaRegistryKafkaSerializationSchema``` to write ```Message``` records to a Kafka topic:

```java
public class AvroDataGeneratorJob {

Map<String, String> sslClientConfig = new HashMap<>();
sslClientConfig.put(K_TRUSTSTORE_PATH, params.get(K_SCHEMA_REG_SSL_CLIENT_KEY + "." + K_TRUSTSTORE_PATH));
sslClientConfig.put(K_TRUSTSTORE_PASSWORD, params.get(K_SCHEMA_REG_SSL_CLIENT_KEY + "." + K_TRUSTSTORE_PASSWORD));

Map<String, Object> schemaRegistryConf = new HashMap<>();
schemaRegistryConf.put(K_SCHEMA_REG_URL, params.get(K_SCHEMA_REG_URL));
schemaRegistryConf.put(K_SCHEMA_REG_SSL_CLIENT_KEY, sslClientConfig);

KafkaSerializationSchema<Message> schema = ClouderaRegistryKafkaSerializationSchema.<Message>
        builder(P_KAFKA_TOPIC)
        .setConfig(schemaRegistryConf)
        .setKey(Message::getId)
        .build();

FlinkKafkaProducer<Message> kafkaSink = new FlinkKafkaProducer<>(
               "default", (KafkaSerializationSchema<Message>) schema,
               properties, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);

}               

```

The ```KafkaToHDFSAvroJob``` uses a Kafka source with ```ClouderaRegistryKafkaDeserializationSchema``` to read the Avro records. Then, it saves them as CSV files into HDFS and standard output:

```java
Map<String, String> sslClientConfig = new HashMap<>();
sslClientConfig.put(K_TRUSTSTORE_PATH, params.get(K_SCHEMA_REG_SSL_CLIENT_KEY + "." + K_TRUSTSTORE_PATH));
sslClientConfig.put(K_TRUSTSTORE_PASSWORD, params.get(K_SCHEMA_REG_SSL_CLIENT_KEY + "." + K_TRUSTSTORE_PASSWORD));

Map<String, Object> schemaRegistryConf = new HashMap<>();
schemaRegistryConf.put(K_SCHEMA_REG_URL, params.get(K_SCHEMA_REG_URL));
schemaRegistryConf.put(K_SCHEMA_REG_SSL_CLIENT_KEY, sslClientConfig);

KafkaDeserializationSchema<Message> schema = ClouderaRegistryKafkaDeserializationSchema
       .builder(Message.class)
       .setConfig(schemaRegistryConf)
       .build();

FlinkKafkaConsumer<Message> consumer = new FlinkKafkaConsumer<Message>(P_KAFKA_TOPIC, schema, properties);
DataStream<String> source = env.addSource(consumer).name("Kafka Source")
       .map(record -> record.getId() + "," + record.getName() + "," + record.getDescription());
source.print();
```

The ```ClouderaRegistryKafkaSerializationSchema/ClouderaRegistryKafkaDeserializationSchema ``` related configuration parameters are shipped in the application.properties file too:
```
schema.registry.url=https://morhidi-sec-1.gce.cloudera.com:7790/api/v1
schema.registry.client.ssl.trustStorePath=/home/test/truststore.jks
schema.registry.client.ssl.trustStorePassword=changeit
```
For Kerberos authentication Flink provides seamless integration for Cloudera Schema Registry through the ```security.kerberos.login.contexts property```. After defining an additional ```RegistryClient``` context here, Flink can maintain authentication and ticket renewal automatically. It is recommended to define these contexts in Cloudera Manager globally:
```security.kerberos.login.contexts=Client,KafkaClient,RegistryClient```

## Sample commands
Here are some sample commands, which can be useful for preparing your CDP environment for running the Tutorial Application.

### Kerberos related commands

Creating a *test* user for submitting Flink jobs using **kadmin.local** in the local Kerberos server:

```
> kadmin.local
kadmin.local:  addprinc test
Enter password for principal "test@<hostname>":
Re-enter password for principal "test@<hostname>":
Principal "test@<hostname>" created.
kadmin.local:  quit
```

Initializing the HDFS home directory for the test user with a superuser:
```
> kinit hdfs
Password for hdfs@<hostname>:
> hdfs dfs -mkdir /user/test
> hdfs dfs -chown test:test /user/test
```

Creating the test user locally (should be added on each node):
```
> useradd test
```

Creating a keytab for the test user with **ktutil**
```
> ktutil
ktutil: add_entry -password -p test -k 1 -e des3-cbc-sha1
Password for test@<hostname>:
ktutil:  wkt test.keytab
ktutil:  quit
```
Listing the stored principal(s) from the keytab:
```
> klist -kte test.keytab
Keytab name: FILE:test.keytab
KVNO Timestamp           Principal
---- ------------------- ------------------------------------------------------
   1 09/19/2019 02:12:18 test@<hostname> (des3-cbc-sha1)
```

Verifying with **kinit** and **klist** if authentication works properly with the keytab:
```
[root@morhidi-1 ~]# klist -e
Ticket cache: FILE:/tmp/krb5cc_0
Default principal: test@<hostname>

Valid starting       Expires              Service principal
09/24/2019 03:49:09  09/24/2019 04:14:09  krbtgt/<hostname@<hostname
	renew until 09/24/2019 05:19:09, Etype (skey, tkt): des3-cbc-sha1, des3-cbc-sha1
```

### Kafka related commands

Creating the *flink* topic in Kafka for testing
```
> kafka-topics --create  --zookeeper <hostname>:2181/kafka --replication-factor 3 --partitions 3 --topic flink
> kafka-topics --zookeeper <hostname>:2181/kafka --list
```

Sending messages to the *flink* topic with **kafka-console-producer**:

```
KAFKA_OPTS="-Djava.security.auth.login.config=.kafka.jaas.conf" kafka-console-producer --broker-list <hostname>:9093 --producer.config .kafka.client.properties --topic flink
```

Reading messages to the *flink* topic with **kafka-console-producer**:
```
KAFKA_OPTS="-Djava.security.auth.login.config=.kafka.jaas.conf" kafka-console-consumer --bootstrap-server <hostname>:9093 --consumer.config .kafka.client.properties --topic flink --from-beginning
```

Kafka configurations are given as separate files (*.kafka.client.properties* and .kafka.jaas.conf) for **kafka-console-producer** and **kafka-console-consumer** commands:
```
> cat .kafka.client.properties
security.protocol=SASL_SSL
sasl.kerberos.service.name=kafka
ssl.truststore.location=/etc/cdep-ssl-conf/CA_STANDARD/truststore.jks
```
Preparing a *.kafka.jaas.conf* file for **kafka-console-producer** to access the secured Kafka service:
```
> cat .kafka.jaas.conf
KafkaClient {
com.sun.security.auth.module.Krb5LoginModule required
useTicketCache=true;
};
```

**Note**
This approach can also be used for testing and verifying the security properties for the Kafka connector used in the Flink application itself.

