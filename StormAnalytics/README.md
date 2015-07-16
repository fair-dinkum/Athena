# StormAnalytics

StormAnalytics is a Storm application that reads a Kafka Twitter topic and analyzes and scores the stream in realtime and archives the raw tweets in HDFS

## To compile:
```sh
mvn clean package
```

## To deploy Storm Topology:
```sh
storm jar StormAnalytics-0.0.1.jar com.hortonworks.storm.analytics.TwitterAnalyticsTopology
```
