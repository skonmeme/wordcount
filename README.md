## WordCount on Flink

###### Open socket with port 9000 
```
nc -l 9000
```

###### Start Flink job
```
flink run -c com.skt.skon.wordcount.WordCount build/libs/wordcount-1.0-SNAPSHOT.jar 
```

Check you taskmanager.
