# SimpleSparkMaprStreams

Step 1: First compile the project  : Go into directory where project is downloaded and do mvn clean package
Step 2 : Login into Sanbox as 'mapr' user and create a directory 
          mkdir /mapr/user/mapr/streams

Create the topics

maprcli stream create -path /user/mapr/streams/pump -produceperm p -consumeperm p -topicperm p
maprcli stream topic create -path /user/mapr/streams/pump -topic sensor -partitions 3

Step 3 :
use scp to copy the JAR  to the mapr sandbox or cluster

Step 4 :
To run the MapR Streams Java producer : java -cp spark_consumer-0.0.1-SNAPSHOT.jar:`mapr classpath` com.streams.spark_consumer.MyProducer

To run the MapR Streams Spark Consumer : java -cp spark_consumer-0.0.1-SNAPSHOT.jar:`mapr classpath` com.streams.spark_consumer.SparkConsumer








