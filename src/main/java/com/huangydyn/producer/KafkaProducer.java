package com.huangydyn.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.huangydyn.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Component
public class KafkaProducer {

    private static final String TOPIC = "haley_topic";

    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(
            "yyyyMMdd hh:mm:ss");

    private Logger logger = LoggerFactory.getLogger(KafkaProducer.class);

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    int key = 0;

    @Scheduled(fixedDelay = 3 * 1000)
    public void sendMessage() throws Exception {
        Message msg = new Message();
        msg.setId(String.valueOf(key));
        msg.setMsg("Hi Spring Kafka");
        msg.setSendTime(LocalDateTime.now().format(formatter));

        kafkaTemplate.send(TOPIC, String.valueOf(key), objectMapper.writeValueAsString(msg))
                .addCallback(o -> logger.info("[Producer] send-message success：" + msg),
                        throwable -> logger.error("[Producer] send-message failed", throwable));
        key++;
    }
}
