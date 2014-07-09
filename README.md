Hadoop Consumer
===============
-Consumes Kafka messages from any provided server and topic, writes them to ~/kafka/ in HDFS

-Writes each received message to a unique line

-Writes 1000 messages per file

Usage
=====
Command line:

"hadoop jar /target/hadoop-consumer-0.0.1-SNAPSHOT-jar-with-dependencies.jar $zookeeperserver:port $consumername $topic"


$zookeeperserver:port - zookeeper server to listen to

$consumername - consumer group name to be used, should be unique in order to receive messages

$topic - topic to consume

----------------------------------------------------------------------------------------------------

Multiple instances can be started to consume multiple topics or consume from different servers

   -Files written to HDFS are named according to their topic

An output file log is created in the local directory ~/kafka/ to keep track of file numbering in case of a crash
