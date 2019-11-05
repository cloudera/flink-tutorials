# Flink tutorial Application for Secured CDH/CDP Clusters

This application demonstrates how to enable essential Flink security features for applications intended to run on secured CDP environments. For the sake of simplicity the application logic here is kept pretty basic, it reads messages from a kafka topic and stores them as is on HDFS.

## Table of Contents
- [Flink Tutorial Application for Secured CDH/CDP Clusters](#flink-tutorial-application-for-secured-cdh-cdp-clusters)
  * [Flink Security in a Nutshell](#flink-security-in-a-nutshell)
    + [Authentication](#authentication)
    + [Encryption (TLS)](#encryption--tls-)
      - [Internal Connectivity](#internal-connectivity)
      - [External Connectivity (REST Endpoints)](#external-connectivity--rest-endpoints-)
  * [Complete Command including Security](#complete-command-including-security)
    + [Preparation](#preparation)
    + [Steps to run the Secured Flink Quickstart Application:](#steps-to-run-the-secured-flink-quickstart-application-)
  * [Understanding Security Parameters:](#understanding-security-parameters-)
    + [application.properties](#applicationproperties)
  * [Kafka Metrics Reporter](#kafka-metrics-reporter)
  * [Schema Registry Integration](#schema-registry-integration)
  * [Prerequisites](#prerequisites)
    + [Kerberos related commands](#kerberos-related-commands)
    + [Kafka related commands](#kafka-related-commands)


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
With this example we are focusing on how to handle authentication and encryption (TLS) in Flink applications.

After cloning an building the project,
```
> git clone https://github.infra.cloudera.com/morhidi/flink-ref.git
> cd flink-sec-tutorial
> mvn clean package
> cd target
```
on **non-secured** CDH/CDP clusters the quick start application would be submitted with the following command:

```
flink run -m yarn-cluster -d -p 2 \
flink-sec-tutorial-1.0-SNAPSHOT.jar \
--kafka.bootstrap.servers <hostname>:9093 \
--kafkaTopic flink \
--hdfsOutput hdfs:///tmp/flink-sec-tutorial
```
As you will see enabling security features make the command a bit more complicated, hence we are trying clarify things with this example.

## Flink Security in a Nutshell

### Authentication
A Flink program may use first- or third-party connectors with required authentication methods (Kerberos, SSL/TLS, username/password, etc.). While satisfying the security requirements for various connectors is an ongoing effort, Flink provides first-class support for Kerberos authentication only.

The primary goals of the Flink Kerberos security infrastructure are:
* to enable secure data access for jobs within a cluster via connectors (e.g. Kafka)
* to authenticate to Hadoop components (e.g. HDFS, HBase, Zookeeper)

In a production deployment scenario, streaming jobs are understood to run for long periods of time (days/weeks/months) and be able to authenticate to secure data sources throughout the life of the job. Kerberos keytabs do not expire in that timeframe, unlike a Hadoop delegation token or ticket cache entry, thus using keytabs is the recommended approach for long running production deployments.

### Encryption (TLS)

Apache Flink differentiates between internal and external connectivity. Internal Connectivity refers to all connections made between Flink processes. External endpoints refers to all connections made from the outside to Flink processes.

#### Internal Connectivity

Because internal communication is mutually authenticated, keystore and truststore typically contain the same dedicated certificate. The certificate can use wild card hostnames or addresses, because the certificate is expected to be a shared secret and host names are not verified.

#### External Connectivity (REST Endpoints)

When Flink applications are running on CDH/CDP clusters, Flink’s web dashboard is accessible through YARN proxy’s Tracking URL. Depending on the security setup in YARN the proxy itself can enforce authentication (SPNEGO) and encryption (TLS) already for YARN jobs. This can be sufficient in such cases where CDP perimeter is protected from external user access by firewall. If there is no such protection additional TLS configuration is required to protect REST endpoints with TLS. Refer to [Flink's upstream documentation](https://ci.apache.org/projects/flink/flink-docs-release-1.9/ops/security-ssl.html#example-ssl-setup-standalone-and-kubernetes) for further details.


## Complete Command including Security

### Preparation
* CDH6.3+/CDP7.0+ clusters with Kerberos(MIT or AD) and TLS integration(Manual or Auto TLS) including services:
  * HDFS
  * Kafka
  * YARN
  * Flink
* Existing test user in the Kerberos realm and as local users on each cluster nodes. You can find some usefull commands for fulfilling the prerequisites in a dedicated chapter [here](#prerequisites)

### Steps to run the Secured Flink tutorial Application:

* For Kerberos authentication generate a keytab file for the user inteded to submit the Flink job. Keytabs can be generated with ktutil, for example:

  ```
  > ktutil
  ktutil: add_entry -password -p test -k 1 -e des3-cbc-sha1
  Password for test@:
  ktutil:  wkt test.keytab
  ktutil:  quit
  ```
* For internal TLS encryption generate a keystore file for the user inteded to run the Flink application. If the JAVA_HOME is not set globally on the host than the keytool can be accessed usually at ```/usr/java/default/bin/keytool```.

  ```
  keytool -genkeypair -alias flink.internal -keystore keystore.jks -dname "CN=flink.internal" -storepass `cat pwd.txt` -keyalg RSA -keysize 4096 -storetype PKCS12
  ```
  The key pair acts as the shared secret for internal security, and we can directly use it as keystore and truststore.

* Submit Flink application as normal using additional security configuration params:
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
  flink-sec-tutorial-1.0-SNAPSHOT.jar \
  --kafkaTopic flink \
  --hdfsOutput hdfs:///tmp/flink-sec-tutorial \
  --kafka.bootstrap.servers <hostname>:9093 \
  --kafka.security.protocol SASL_SSL \
  --kafka.sasl.kerberos.service.name kafka \
  --kafka.ssl.truststore.location /etc/cdep-ssl-conf/CA_STANDARD/truststore.jks
  ```
* Send some messages to the *flink* topic in Kafka and check the application logs and the HDFS output folder to verify the messages arrive there as expected.

## Understanding Security Parameters:

Authentication related configs are defined as Flink command line parameters(-yD):

```
-yD security.kerberos.login.keytab=test.keytab
-yD security.kerberos.login.principal=test
```

Internal network encryption related configs are defined as Flink command line parameters(-yD):
```
-yD security.ssl.internal.enabled=true
-yD security.ssl.internal.keystore=keystore.jks
-yD security.ssl.internal.key-password=`cat pwd.txt`
-yD security.ssl.internal.keystore-password=`cat pwd.txt`
-yD security.ssl.internal.truststore=keystore.jks
-yD security.ssl.internal.truststore-password=`cat pwd.txt`
-yt keystore.jks
```

Kafka connector properties defined as normal job arguments (since there are no built-in configs for these in Flink ):

```
--kafka.security.protocol SASL_SSL \
--kafka.sasl.kerberos.service.name kafka \
--kafka.ssl.truststore.location /etc/cdep-ssl-conf/CA_STANDARD/truststore.jks
```

Any number of kafka connector properties can be added to the command dynamically using the *"kafka."* prefix. These properties are forwarded to the kafka consumer after trimming the *"kafka."* prefix from them:
```java
  Properties properties = new Properties();
        for (String key : params.getProperties().stringPropertyNames()) {
            if (key.startsWith(KAFKA_PREFIX)) {
                properties.setProperty(key.substring(KAFKA_PREFIX.length()), params.get(key));
            }
        }
  FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(P_KAFKA_TOPIC, new SimpleStringSchema(), properties);
```
Please note that the trustore given for the Kafka connector is different from the one we generated for Flink internal encryption above. This is the truststore we need to access the TLS protected Kafka endpoint.

The keytab and the keystore files that are referred as ```-yD security.kerberos.login.keytab=test.keytab``` and ```-yt keystore.jks``` respectively are distributed automatically to the YARN container's temporary folder on the remote hosts. These properties are user specific, thus usually cannot be added to default Flink configuration unless a single technical user us used to submit the flink jobs.  If there is a dedicated technical user for submitting Flink jobs on a cluster the keytab and keystore files can be provisioned to the YARN hosts in advance and related configuration params can be set globally in CM using Safety Valves.

For further reading check related chapters from **Flink Security Documentation**:
Check the [Kerberos](https://ci.apache.org/projects/flink/flink-docs-release-1.9/ops/security-kerberos.html) and [TLS](https://ci.apache.org/projects/flink/flink-docs-release-1.9/ops/security-ssl.html) topics from upstream Flink documentation for further details. Kafka connector related security settings can be also accessed [here](https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/connectors/kafka.html#enabling-kerberos-authentication-for-versions-09-and-above-only).


### application.properties
As you can see the number of security related configuration options with various Flink connectors can go wild pretty quickly. Hence, it is recommended to define as much property as possible in a separate config file and submit it along other business parameters.

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
flink-sec-tutorial-1.0-SNAPSHOT.jar \
--properties.file application.properties
```

Flink has a builtin *ParameterTool* class to handle program arguments elegantly. You can merge the arguments given in the command line and configuration file for example:
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

## Kafka Metrics Reporter
There is a metrics reporter implementation for writing Flink metrics to a target Kafka topic in JSON format. The corresponding security related Kafka properties can be set either in the command itself or globally via CM safety valve (Flink Client Advanced Configuration Snippet (Safety Valve) for flink-conf-xml/flink-cli-conf.xml)
```
-yD metrics.reporter.kafka.class=org.apache.flink.metrics.kafka.KafkaMetricsReporter \
-yD metrics.reporter.kafka.topic=metrics-topic.log \
-yD metrics.reporter.kafka.bootstrap.servers=morhidi-1.gce.cloudera.com:9093 \
-yD metrics.reporter.kafka.security.protocol=SASL_SSL \
-yD metrics.reporter.kafka.sasl.kerberos.service.name=kafka \
-yD metrics.reporter.kafka.ssl.truststore.location=/etc/cdep-ssl-conf/CA_STANDARD/truststore.jks \
```

## Schema Registry Integration

Flink Kafka sources and sinks can be used together with Cloudera Schema Registry to maintain schema information about your Kafka topics. In this example the schema registry is used to provide automatic serialization/deserialization functionality for Kafka Avro messages.

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
The schema of the Avro types are registered in the Cloudera Schema Registry automatically on first record serialization.

The schema registry can be plugged directly into a ```FlinkKafkaConsumer``` or a ```FlinkKafkaProducer``` using the appropriate ```SchemaRegistryDeserializationSchema``` and ```SchemaRegistrySerializationSchema``` classes respectively.

Two sample Jobs were written to demonstrate how to integrate with Cloudera Schema Registry from Flink in a secured way.


The ```AvroDataGeneratorJob``` uses a Kafka sink with ```SchemaRegistrySerializationSchema``` to write ```Message``` records to a Kafka topic:

```java
public class AvroDataGeneratorJob {

Map<String, String> sslClientConfig = new HashMap<>();
sslClientConfig.put(K_TRUSTSTORE_PATH, params.get(K_SCHEMA_REG_SSL_CLIENT_KEY + "." + K_TRUSTSTORE_PATH));
sslClientConfig.put(K_TRUSTSTORE_PASSWORD, params.get(K_SCHEMA_REG_SSL_CLIENT_KEY + "." + K_TRUSTSTORE_PASSWORD));

Map<String, Object> schemaRegistryConf = new HashMap<>();
schemaRegistryConf.put(K_SCHEMA_REG_URL, params.get(K_SCHEMA_REG_URL));
schemaRegistryConf.put(K_SCHEMA_REG_SSL_CLIENT_KEY, sslClientConfig);

KafkaSerializationSchema<Message> schema = SchemaRegistrySerializationSchema.<Message>
        builder(P_KAFKA_TOPIC)
        .setConfig(schemaRegistryConf)
        .setKey(Message::getId)
        .build();

FlinkKafkaProducer<Message> kafkaSink = new FlinkKafkaProducer<>(
               "default", (KafkaSerializationSchema<Message>) schema,
               properties, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);

}               

```

The ```KafkaToHDFSAvroJob``` uses a Kafka source with ```SchemaRegistryDeserializationSchema``` to read the Avro records. Then it saves them as CSV files onto HDFS and standard output:

```java
Map<String, String> sslClientConfig = new HashMap<>();
sslClientConfig.put(K_TRUSTSTORE_PATH, params.get(K_SCHEMA_REG_SSL_CLIENT_KEY + "." + K_TRUSTSTORE_PATH));
sslClientConfig.put(K_TRUSTSTORE_PASSWORD, params.get(K_SCHEMA_REG_SSL_CLIENT_KEY + "." + K_TRUSTSTORE_PASSWORD));

Map<String, Object> schemaRegistryConf = new HashMap<>();
schemaRegistryConf.put(K_SCHEMA_REG_URL, params.get(K_SCHEMA_REG_URL));
schemaRegistryConf.put(K_SCHEMA_REG_SSL_CLIENT_KEY, sslClientConfig);

KafkaDeserializationSchema<Message> schema = SchemaRegistryDeserializationSchema
       .builder(Message.class)
       .setConfig(schemaRegistryConf)
       .build();

FlinkKafkaConsumer<Message> consumer = new FlinkKafkaConsumer<Message>(P_KAFKA_TOPIC, schema, properties);
DataStream<String> source = env.addSource(consumer).name("Kafka Source")
       .map(record -> record.getId() + "," + record.getName() + "," + record.getDescription());
source.print();
```

The ```SchemaRegistrySerializationSchema/SchemaRegistryDeserializationSchema``` related configuration params are shipped in the application.properties file too:
```
schema.registry.url=https://morhidi-sec-1.gce.cloudera.com:7790/api/v1
schema.registry.client.ssl.trustStorePath=/home/test/truststore.jks
schema.registry.client.ssl.trustStorePassword=changeit
```
For Kerberos authentication Flink provides seamless integration for Cloudera Schema Registry via the ```security.kerberos.login.contexts``` property. After defining an additional ```RegistryClient``` context here Flink can maintain authentication and ticket renewal automatically. It is recommended to define these contexts in Cloudera Manager globally:
```security.kerberos.login.contexts=Client,KafkaClient,RegistryClient```

## Prerequisites
Here are some sample commands which can be useful for preparing your CDH/CDP environment for running the tutorial application.

### Kerberos related commands

Creating a *test* user for submitting flink jobs using **kadmin.local** in the local Kerberos server:

```
> kadmin.local
kadmin.local:  addprinc test
Enter password for principal "test@<hostname>":
Re-enter password for principal "test@<hostname>":
Principal "test@<hostname>" created.
kadmin.local:  quit
```

Initializing the HDFS home directory for the *test* user with a superuser (In CDEP environments the superuser is the hdfs user, which should be automatically created during cluster provisioning, the password is the default CDEP password):
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
This approach is also useful for testing and verifying the security properties for the Kafka connector used in the Flink application itself.
