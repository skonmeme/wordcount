## WordCount on Flink

###### Create kafka 3 topics 
```
kafka-topics.sh --zookeeper localhost:2181 --create --topic wordcount-text --partitions 1 --replication-factor 1
kafka-topics.sh --zookeeper localhost:2181 --create --topic wordcount-words --partitions 1 --replication-factor 1
kafka-topics.sh --zookeeper localhost:2181 --create --topic wordcount-another --partitions 1 --replication-factor 1
```

###### Start Flink job
```
flink run -c com.skt.skon.wordcount.WordCount build/libs/wordcount-1.0-SNAPSHOT.jar 
```

Check you taskmanager.
