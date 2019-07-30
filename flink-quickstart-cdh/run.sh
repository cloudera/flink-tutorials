ssh -t root@flink-ref-1.gce.cloudera.com JAVA_HOME=/usr/java/jdk1.8.0_141-cloudera/ flink \
	run -sae -m yarn-cluster -p 2 -c com.cloudera.streaming.examples.flink.StreamingJob flink-quickstart-cdh-1.0-SNAPSHOT.jar
