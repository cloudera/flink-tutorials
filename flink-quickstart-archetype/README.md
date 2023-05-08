# Flink Quickstart Archetype
This is a maven archetype for generating Java projects for Flink on Cloudera.

## Prerequisites
Cloudera does not release maven archetypes to the Maven Central Repository thus the archetype should be installed locally on your host before usage.

```shell
git clone https://github.com/cloudera/flink-tutorials
cd flink-quickstart-archetype
mvn install
cd ..
```

When complete an entry is added to your local archetype catalog:
```
cat ~/.m2/repository/archetype-catalog.xml
 ...
 <archetypes>
    <archetype>
      <groupId>com.cloudera.flink</groupId>
      <artifactId>flink-quickstart-archetype</artifactId>
      <version>1.16.1-csadh1.11.0.0-SNAPSHOT</version>
      <description>flink-quickstart-archetype</description>
    </archetype>
  </archetypes>
  ...
```

## Usage
Once the archetype is installed you can generate project skeletons by running:
```shell
mvn archetype:generate \
  -DarchetypeGroupId=com.cloudera.flink \
  -DarchetypeArtifactId=flink-quickstart-archetype \
  -DarchetypeVersion=1.16.1-csadh1.11.0.0-SNAPSHOT
```

You must provide some basic information about your new project when prompted.
```
Define value for property 'groupId': com.cloudera.flink
Define value for property 'artifactId': sample-project
Define value for property 'version' 1.0-SNAPSHOT: :
Define value for property 'package' com.cloudera.flink: :
Confirm properties configuration:
groupId: com.cloudera.flink
artifactId: sample-project
version: 1.0-SNAPSHOT
package: com.cloudera.flink
Y: :
```

> **Note:** By hitting 'Enter' you can accept default values.

The generated project structure will look like the following:
```
sample-project
├── pom.xml
└── src
    └── main
        ├── java
        │   └── com
        │       └── cloudera
        │           └── flink
        │               └── StreamingJob.java
        └── resources
            └── log4j.properties
```
