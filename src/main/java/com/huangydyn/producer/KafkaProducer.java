package com.huangydyn.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.huangydyn.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Component
public class KafkaProducer {
    private static final String TOPIC = "test";
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd hh:mm:ss");
    private Logger logger = LoggerFactory.getLogger(KafkaProducer.class);
    @Autowired
    private KafkaTemplate kafkaTemplate;
    @Autowired
    private ObjectMapper objectMapper;

    int i = 0;

    //@Scheduled(fixedDelay = 5 * 1000)
    public void sendMessage() throws Exception {
        Message msg = new Message();
        msg.setId(String.valueOf(i));
        i++;
        msg.setMsg("Hi Spring Kafka");
        msg.setSendTime(LocalDateTime.now().format(formatter));

        ListenableFuture future = kafkaTemplate.send(TOPIC, objectMapper.writeValueAsString(msg));

        future.addCallback(o -> logger.info("send-message success：" + msg.toString()), throwable -> logger.info("send-message failed" + throwable.getMessage()));
    }
}
