package com.huangydyn.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.huangydyn.model.Message;
import com.huangydyn.producer.KafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
public class KafkaConsumer {

    private static final String TOPIC = "test";

    private Logger logger = LoggerFactory.getLogger(KafkaProducer.class);

    private ObjectMapper objectMapper;

    public KafkaConsumer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = {TOPIC}, group = "group_test")
    public void listenTest(ConsumerRecord<?, ?> record, Acknowledgment acknowledgment) throws Exception {
        Optional<?> kafkaMessage = Optional.ofNullable(record.value());
        if (kafkaMessage.isPresent()) {
            Object content = kafkaMessage.get();
            Message msg = objectMapper.readValue(content.toString(), Message.class);
            logger.info("receive msg, offset {},body {}", record.offset(), msg.toString());
            // 测试消费失败后消息丢失
            if (msg.getId().equals("3")) {
                throw new RuntimeException("failed");
            }
            logger.info("commit msg successful, offset {}", record.offset());
            acknowledgment.acknowledge();
        }
    }


}

