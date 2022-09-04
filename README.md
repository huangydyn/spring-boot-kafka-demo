# spring boot kafka
- spring boot kafka integration
- kafka docker

# stream流式计算实例
## KafkaTemperature
```
/opt/kafka/bin/kafka-topics.sh  --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic iot-temperature
/opt/kafka/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic iot-temperature-max

# 生产消息
/opt/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic iot-temperature

# 接收topic
/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic iot-temperature-max --from-beginning
```
## WordCount
```
/opt/kafka/bin/kafka-topics.sh  --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic wordcount-output-topic
/opt/kafka/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic wordcount-input-topic

# 生产消息
/opt/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic wordcount-input-topic

# 接收topic
/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic wordcount-output-topic \
    --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true \
    --property print.value=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
```
