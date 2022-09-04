package com.huangydyn.consumer.java;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.huangydyn.model.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

public class JavaConsumer {
    private static org.slf4j.Logger logger = LoggerFactory.getLogger(JavaConsumer.class);

    private static ObjectMapper objectMapper = new ObjectMapper();

    static {
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public static void main(String[] args) {
        Logger root = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.INFO);

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test-consumer-group");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("test"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1);
            for (ConsumerRecord<String, String> record : records) {
                Message msg = new Message();
                try {
                    msg = objectMapper.readValue(record.value().toString(), Message.class);
                } catch (Exception e) {
                    logger.info("error parse");
                }
                logger.info("receive msg, offset {},body {}", record.offset(), msg.toString());
                if (msg.getId().equals("3")) {
                    logger.info("not commit offset, {}", record.offset());
                } else {
                     consumer.commitSync();
                     logger.info("commit msg successful, offset {}", record.offset());
                }
            }
        }
    }
}
