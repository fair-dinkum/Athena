# TwitterKafkaProducer

TwitterKafkaProducer is a Kafka producer that reads off the twitter firehose and populates a kafka topic

## To compile:
```sh
mvn clean package
```

## To create Kafka Topic:
```sh
/usr/hdp/current/kafka-broker/bin
./kafka-topics.sh --create --zookeeper xxxx.us-west-1.compute.internal:2181 --replication-factor 3 --partitions 3 --topic tweets
```

## To run:
```sh
nohup java -jar TwitterKafkaProducer-0.0.1.jar 2>&1
```

## To view kafka queue:
```sh
./kafka-console-consumer.sh --zookeeper localhost --topic tweets
```
