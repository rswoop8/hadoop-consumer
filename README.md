#Hadoop Consumer

 * Consumes Kafka messages from any provided server and topic, writes them to a file at ~/kafka/ in HDFS
 * Writes 1000 messages per file, with each message on a unique line

##Overview

 * First writes files locally, until 1000 messages have been received
 * After the thousandth message is received, the file is written to HDFS and deleted locally
 * A log file is kept locally at ~/kafka/ to maintain correct numbering of files in case of a crash

##Usage

To build:
```
mvn clean package
```

To run:
```
hadoop jar /target/hadoop-consumer-0.0.1-SNAPSHOT-jar-with-dependencies.jar $zookeeperserver:port $consumername $topic
```

Arguments:

`$zookeeperserver:port - zookeeper server to connect to`

`$consumername - consumer group name to be used, should be unique in order to receive messages`

`$topic - topic to consume`

----------------------------------------------------------------------------------------------------

Multiple instances can be started to consume multiple topics or consume from different servers

 * Files written to HDFS are named according to their topic
