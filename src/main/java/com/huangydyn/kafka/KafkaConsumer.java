package com.huangydyn.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.huangydyn.model.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
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

    @KafkaListener(topics = {TOPIC})
    public void listen(ConsumerRecord<?, ?> record) throws Exception {
        Optional<?> kafkaMessage = Optional.ofNullable(record.value());
        if (kafkaMessage.isPresent()) {
            Object content = kafkaMessage.get();
            Message msg = objectMapper.readValue(content.toString(), Message.class);

            logger.debug(msg.toString());
        }
    }
}

