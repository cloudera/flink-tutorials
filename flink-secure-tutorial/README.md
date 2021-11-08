# Flink Application Tutorial for Secured CDP PVC Base Clusters

## Table of Contents

 1. [Overview](#overview)
 2. [Build](#build)
     + [Prerequisites](#prerequisites)
 3. [Understanding security parameters](#understanding-security-parameters)
 4. [Submitting Flink jobs with full security](#submitting-flink-jobs-with-full-security)
     + [Preparation](#preparation)
     + [Steps to run the secured Flink application tutorial](#steps-to-run-the-secured-flink-application-tutorial)
     + [Job properties](#jobproperties)
 5. [Kafka metrics reporter](#kafka-metrics-reporter)
 6. [Schema Registry integration](#schema-registry-integration)
 7. [Sample commands](#sample-commands)
     + [Kerberos related commands](#kerberos-related-commands)
     + [Kafka related commands](#kafka-related-commands)

## Overview

This tutorial demonstrates how to enable essential Flink security features (e.g. Kerberos and TLS) for applications that can run in secured CDP PVC Base (CDP for short) environments. Since, we are focusing mainly on security features here the sample application that we use is pretty basic. Our flink job reads messages from a Kafka topic and stores them on HDFS:

```java
public class KafkaToHDFSSimpleJob {

    public static void main(String[] args) throws Exception {

        ...

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                params.getRequired("kafkaTopic"), new SimpleStringSchema(),
                Utils.readKafkaProperties(params));
        DataStream<String> source = env.addSource(consumer)
                .name("Kafka Source")
                .uid("Kafka Source");

        StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(new Path(P_FS_OUTPUT), new SimpleStringEncoder<String>("UTF-8"))
                .build();

        source.addSink(sink)
                .name("FS Sink")
                .uid("FS Sink");
        source.print();

        env.execute("Flink Streaming Secured Job Sample");
 }
}
```
We will introduce the security configs gradually that makes it easier to consume. For more information about Flink Security, see the section [Security Overview](https://docs.cloudera.com/csa/latest/security/topics/csa-authentication.html) in Cloudera Streaming Analytics document.

## Build

### Prerequisites

You need to [create a topic called `flink`](#kafka-related-commands).
Also, don't forget to [set up your HDFS home directory](https://docs.cloudera.com/csa/latest/installation/topics/csa-hdfs-home-install.html) if you haven't done it yet. Once the dependencies are in place we can build the project:
```shell
cd flink-tutorials/flink-secure-tutorial
mvn clean package
cd target
```

On a typical *NON-SECURED* CDP cluster, the command to start our flink job would look something like this:
```shell
flink run -d -ynm SecureTutorial flink-secure-tutorial-1.14.0-csa1.6.0.0-SNAPSHOT.jar \
  --kafka.bootstrap.servers "<your-broker>":9092 \
  --kafkaTopic flink
```

> **Note:** The tutorial uses the flink command line parameters in short form, to see all the options run `flink run -h`. The default non-secured kafka port is 9092, the default TLS port is 9093.

## Understanding security parameters

In a production deployment scenario, streaming jobs are understood to run for long periods of time and be able to authenticate to secure data sources throughout the life of the job. Kerberos keytabs do not expire in that timeframe, thus recommended in flink for authentication. Kerberos related configurations are defined as Flink command line parameters (`-yD`):
```
-yD security.kerberos.login.keytab=test.keytab
-yD security.kerberos.login.principal=test
```

Apache Flink differentiates between internal and external connectivity. All internal connections can be SSL authenticated and encrypted. TLS related configurations are also defined as Flink command line parameters (`-yD`):
```
-yD security.ssl.internal.enabled=true
-yD security.ssl.internal.keystore=keystore.jks
-yD security.ssl.internal.key-password=******
-yD security.ssl.internal.keystore-password=******
-yD security.ssl.internal.truststore=keystore.jks
-yD security.ssl.internal.truststore-password=******
-yt keystore.jks
```
> **Note:** External TLS configuration is out of scope of this tutorial

Kafka connector security properties should be defined as normal job arguments, since there are no built-in configurations for them in Flink:
```
--kafka.security.protocol SASL_SSL \
--kafka.sasl.kerberos.service.name kafka \
--kafka.ssl.truststore.location /var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_truststore.jks
```

In our sample application any number of Kafka connector properties can be added to the command dynamically using the `kafka.` prefix. The application forwards them to the Kafka consumer after trimming the `kafka.` prefix from them:
```java
Properties properties = new Properties();
for (String key : params.getProperties().stringPropertyNames()) {
    if (key.startsWith(KAFKA_PREFIX)) {
        properties.setProperty(key.substring(KAFKA_PREFIX.length()), params.get(key));
    }
}

...

FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
        params.getRequired("kafkaTopic"), new SimpleStringSchema(),
        Utils.readKafkaProperties(params));
```
> **Note:** The truststore given for the Kafka connector for example is different from the one generated for Flink internal encryption. This is the truststore used to access the TLS protected Kafka endpoint. For more security information, see the Apache Flink documentation about [Kerberos](https://ci.apache.org/projects/flink/flink-docs-release-1.13/deployment/security/security-kerberos.html), [TLS](https://ci.apache.org/projects/flink/flink-docs-release-1.13/deployment/security/security-ssl.html) and [Kafka](https://ci.apache.org/projects/flink/flink-docs-release-1.13/dev/connectors/kafka.html#enabling-kerberos-authentication) connector.

## Submitting Flink jobs with full security

### Preparation

* Cloudera Runtime 7.0+ clusters with Kerberos (MIT or AD) and TLS integration (Manual or Auto TLS) including services:
  * HDFS
  * Kafka
  * YARN
  * Flink
* Existing test user in the Kerberos realm and as local users on each cluster nodes. You can find some useful commands for fulfilling the prerequisites in a dedicated chapter [here](#sample-commands).

### Steps to tun the secured Flink application tutorial

1. For Kerberos authentication, generate a keytab file for the user intended to submit the Flink job. Keytabs can be generated with `ktutil` as the following example:
```
> ktutil
ktutil: add_entry -password -p test -k 1 -e des3-cbc-sha1
Password for test@:
ktutil:  wkt test.keytab
ktutil:  quit
```
2. For internal TLS encryption generate a keystore file for the user intended to run the Flink application. If the `JAVA_HOME` is not set globally on the host, then the keytool can be usually accessed at `/usr/java/default/bin/keytool`.
```shell
keytool -genkeypair -alias flink.internal -keystore keystore.jks -dname "CN=flink.internal" -storepass ****** -keyalg RSA -keysize 4096 -storetype PKCS12
```

The key pair acts as the shared secret for internal security, and we can directly use it as keystore and truststore.

3. If Ranger is also present the test user should be authorized to publish to the flink topic. This can be done on the Ranger UI. Navigate to Kafka, click on the `Add New Policy` button, then configure it like this:

![Ranger Settings](images/RangerSettings.png "Ranger Settings")

4. Using additional security configuration parameters, submit Flink application as normal:
```shell
flink run -d -ynm SecureTutorial \
  -yD security.kerberos.login.keytab=test.keytab \
  -yD security.kerberos.login.principal=test \
  -yD security.ssl.internal.enabled=true \
  -yD security.ssl.internal.keystore=keystore.jks \
  -yD security.ssl.internal.key-password=****** \
  -yD security.ssl.internal.keystore-password=****** \
  -yD security.ssl.internal.truststore=keystore.jks \
  -yD security.ssl.internal.truststore-password=****** \
  -yt keystore.jks \
  flink-secure-tutorial-1.14.0-csa1.6.0.0-SNAPSHOT.jar \
  --kafkaTopic flink \
  --hdfsOutput hdfs:///tmp/flink-sec-tutorial \
  --kafka.bootstrap.servers <your-broker-1>:9093 \
  --kafka.security.protocol SASL_SSL \
  --kafka.sasl.kerberos.service.name kafka \
  --kafka.ssl.truststore.location /var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_truststore.jks
 ```
5. Send some messages to the `flink` topic in Kafka.
6. Check the application logs and HDFS output folder to verify that messages arrive as expected.

### job.properties

As you can see, the number of security related configuration options can make our `flink run` command fairly complicated pretty quickly.

```shell
flink run -d -ynm SecureTutorial \
  -yD security.kerberos.login.keytab=test.keytab \
  -yD security.kerberos.login.principal=test \
  -yD security.ssl.internal.enabled=true \
  -yD security.ssl.internal.keystore=keystore.jks \
  -yD security.ssl.internal.key-password=****** \
  -yD security.ssl.internal.keystore-password=****** \
  -yD security.ssl.internal.truststore=keystore.jks \
  -yD security.ssl.internal.truststore-password=****** \
  -yt keystore.jks \
  flink-secure-tutorial-1.14.0-csa1.6.0.0-SNAPSHOT.jar \
  --kafkaTopic flink \
  --hdfsOutput hdfs:///tmp/flink-sec-tutorial \
  --kafka.bootstrap.servers <your-broker-1>:9093 \
  --kafka.security.protocol SASL_SSL \
  --kafka.sasl.kerberos.service.name kafka \
  --kafka.ssl.truststore.location /var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_truststore.jks
```
Therefore, it is recommended to ship connector related security properties along with other business properties in a separate configuration file (e.g. `job.properties`):

```shell
cat << "EOF" > job.properties
kafkaTopic=flink
hdfsOutput=hdfs:///tmp/flink-sec-tutorial
kafka.bootstrap.servers=<your-broker-1>:9093
kafka.security.protocol=SASL_SSL
kafka.sasl.kerberos.service.name=kafka
kafka.ssl.truststore.location=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_truststore.jks
EOF
```
Our command looks a bit better now:
```shell
flink run -d -ynm SecureTutorial \
  -yD security.kerberos.login.keytab=test.keytab \
  -yD security.kerberos.login.principal=test \
  -yD security.ssl.internal.enabled=true \
  -yD security.ssl.internal.keystore=keystore.jks \
  -yD security.ssl.internal.key-password=****** \
  -yD security.ssl.internal.keystore-password=****** \
  -yD security.ssl.internal.truststore=keystore.jks \
  -yD security.ssl.internal.truststore-password=****** \
  -yt keystore.jks \
  flink-secure-tutorial-1.14.0-csa1.6.0.0-SNAPSHOT.jar \
  --properties.file job.properties
  ```
> **Note:** We can also simplify our command further if we leave out the optional TLS configs
```shell
flink run -d -ynm SecureTutorial \
  -yD security.kerberos.login.keytab=test.keytab \
  -yD security.kerberos.login.principal=test \
  flink-secure-tutorial-1.14.0-csa1.6.0.0-SNAPSHOT.jar \
  --properties.file job.properties
  ```

### Flink default configs (/etc/flink/conf/flink-conf.yaml)

The keytab and the keystore files that are referred to as `-yD security.kerberos.login.keytab=test.keytab` and `-yt keystore.jks` respectively are distributed automatically to the temporary folder of the YARN container on the remote hosts. These properties are user specific, and usually cannot be added to the default Flink configuration unless a single technical user is used to submit the Flink jobs. If there is a dedicated technical user for submitting Flink jobs on a cluster, the keytab and keystore files can be provisioned to the YARN hosts in advance. In this case the related configuration parameters can be set globally in Cloudera Manager using Safety Valves.

### ParameterTool

Flink has a built-in `ParameterTool` class to handle program arguments elegantly. You can merge the arguments given in the command line and configuration file like the example below:
```java
ParameterTool params = ParameterTool.fromArgs(args);
String propertiesFile = params.get("properties.file");

if (propertiesFile != null) {
    params = ParameterTool.fromProperties(propertiesFile).mergeWith(params);
}

LOG.info("### Job parameters:");
for (String key : params.getProperties().stringPropertyNames()) {
    LOG.info("Param: {}={}", key, params.get(key));
}

String kafkaTopic = params.get("kafkaTopic");
String hdfsOutput = params.get("hdfsOutput");
```

## Kafka metrics reporter

There is a metrics reporter implementation for writing Flink metrics to a target Kafka topic in JSON format. The corresponding security related Kafka properties can be set either in the command itself or globally using Cloudera Manager safety valve (Flink Client Advanced Configuration Snippet for flink-conf-xml/flink-cli-conf.xml).
```
-yD metrics.reporter.kafka.class=org.apache.flink.metrics.kafka.KafkaMetricsReporter \
-yD metrics.reporter.kafka.topic=metrics-topic.log \
-yD metrics.reporter.kafka.bootstrap.servers=<your-broker-1>:9093 \
-yD metrics.reporter.kafka.security.protocol=SASL_SSL \
-yD metrics.reporter.kafka.sasl.kerberos.service.name=kafka \
-yD metrics.reporter.kafka.ssl.truststore.location=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_truststore.jks \
```

## Schema Registry integration

Flink Kafka sources and sinks can be used together with Cloudera Schema Registry to maintain schema information about your Kafka topics. In this example, the schema registry is used to provide automatic serialization/deserialization functionality for Kafka Avro messages.

The ```Message``` Avro type is a Java class generated from the Avro schema ```message.avsc``` during compile time:
```json
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

    public static void main(String[] args) throws Exception {

        ...

        String topic = params.getRequired(K_KAFKA_TOPIC);
        KafkaSerializationSchema<Message> schema = ClouderaRegistryKafkaSerializationSchema
                .<Message>builder(topic)
                .setConfig(Utils.readSchemaRegistryProperties(params))
                .setKey(Message::getId)
                .build();

        FlinkKafkaProducer<Message> kafkaSink = new FlinkKafkaProducer<>(
                topic, schema, Utils.readKafkaProperties(params),
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);

        ...
    }
}
```

The ```KafkaToHDFSAvroJob``` uses a Kafka source with ```ClouderaRegistryKafkaDeserializationSchema``` to read the Avro records. Then, it saves them as CSV files into HDFS and standard output:
```java
public class KafkaToHDFSAvroJob {

    public static void main(String[] args) throws Exception {

        ...

        KafkaDeserializationSchema<Message> schema = ClouderaRegistryKafkaDeserializationSchema
                .builder(Message.class)
                .setConfig(Utils.readSchemaRegistryProperties(params))
                .build();

        FlinkKafkaConsumer<Message> kafkaSource = new FlinkKafkaConsumer<>(
                params.getRequired(K_KAFKA_TOPIC), schema,
                Utils.readKafkaProperties(params));

        DataStream<String> source = env.addSource(kafkaSource)
                .name("Kafka Source")
                .uid("Kafka Source")
                .map(record -> record.getId() + "," + record.getName() + "," + record.getDescription())
                .name("ToOutputString");
        ...
    }
}
```

The ```ClouderaRegistryKafkaSerializationSchema/ClouderaRegistryKafkaDeserializationSchema ``` related configuration parameters can be shipped in the job.properties file too:
```properties
schema.registry.url=https://<your-sr-host>:7790/api/v1
schema.registry.client.ssl.trustStorePath=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_truststore.jks
schema.registry.client.ssl.trustStorePassword=******
```
Once the schema registry properties has been added to `job.properties` the `AvroDataGeneratorJob` can be submitted with:

```shell
flink run -d -ynm AvroDataGeneratorJob \
-yD security.kerberos.login.keytab=morhidi.keytab \
-yD security.kerberos.login.principal=morhidi \
-c com.cloudera.streaming.examples.flink.AvroDataGeneratorJob \
flink-secure-tutorial-1.14.0-csa1.6.0.0-SNAPSHOT.jar \
--properties.file job.properties
```
The generated avro messages can be read by the `KafkaToHDFSAvroJob`

```shell
flink run -d -ynm KafkaToHDFSAvroJob \
-yD security.kerberos.login.keytab=morhidi.keytab \
-yD security.kerberos.login.principal=morhidi \
-c com.cloudera.streaming.examples.flink.KafkaToHDFSAvroJob \
flink-secure-tutorial-1.14.0-csa1.6.0.0-SNAPSHOT.jar \
--properties.file job.properties
```

For Kerberos authentication Flink provides seamless integration for Cloudera Schema Registry through the `security.kerberos.login.contexts` property:
```properties
security.kerberos.login.contexts=Client,KafkaClient,RegistryClient
```

> **Note:**  If Ranger is deployed on your cluster make sure the `test` user has access to the schema groups and schema metadata through the `cm_schema-registry` policy group

## Sample commands

Here are some sample commands, which can be useful for preparing your CDP environment for running the Tutorial Application.

### Kerberos related commands

Creating a **test** user for submitting Flink jobs using `kadmin.local` in the local Kerberos server:
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

Creating a keytab for the test user with `ktutil`:
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

Verifying with `kinit` and `klist` if authentication works properly with the keytab:
```
[root@morhidi-1 ~]# klist -e
Ticket cache: FILE:/tmp/krb5cc_0
Default principal: test@<hostname>

Valid starting       Expires              Service principal
09/24/2019 03:49:09  09/24/2019 04:14:09  krbtgt/<hostname@<hostname
    renew until 09/24/2019 05:19:09, Etype (skey, tkt): des3-cbc-sha1, des3-cbc-sha1
```

### Kafka related commands

Creating the `flink` topic in Kafka for testing:
```shell
kafka-topics --command-config=kafka.client.properties --bootstrap-server `hostname`:9093  --create --replication-factor 3 --partitions 3 --topic flink
kafka-topics --command-config=kafka.client.properties --bootstrap-server `hostname`:9093 --list
```

Sending messages to the *flink* topic with **kafka-console-producer**:

```shell
kafka-console-producer --broker-list  `hostname`:9093 --producer.config kafka.client.properties --topic flink
```

Reading messages from the *flink* topic with **kafka-console-consumer**:
```shell
kafka-console-consumer --bootstrap-server `hostname`:9093  --consumer.config kafka.client.properties --topic flink --from-beginning
```

Kafka configurations are loaded from a *kafka.client.properties* file:
```shell
cat << "EOF" > kafka.client.properties
security.protocol=SASL_SSL
ssl.truststore.location=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_truststore.jks
sasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required \
   serviceName=kafka \
   useTicketCache=true;
EOF
```

Preparing a *.kafka.jaas.conf* file for **kafka-console-producer** to access the secured Kafka service:
```shell
> cat .kafka.jaas.conf
KafkaClient {
com.sun.security.auth.module.Krb5LoginModule required
useTicketCache=true;
};
```

> **Note:** This approach can also be used for testing and verifying the security properties for the Kafka connector used in the Flink application itself.
