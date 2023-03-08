package com.huangydyn.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.huangydyn.consumer.canal.CanalBean;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;

//@Component
public class CanalConsumer {

    private Logger log = LoggerFactory.getLogger(CanalConsumer.class);

    private ObjectMapper objectMapper;

    public CanalConsumer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    //监听的队列名称为：canaltopic
    @KafkaListener(topics = "canal_mysql", groupId = "mysql_consumer")
    public void receive(ConsumerRecord<?, ?> consumer) throws Exception {
        String value = (String) consumer.value();
        log.info("topic名称:{},key:{},分区位置:{},下标:{},value:{}", consumer.topic(), consumer.key(),
                consumer.partition(), consumer.offset(), value);
        //转换为javaBean
        CanalBean canalBean = objectMapper.readValue(value, CanalBean.class);
        //获取是否是DDL语句
        boolean isDdl = canalBean.isDdl();
        //获取类型
        String type = canalBean.getType();
        //不是DDL语句
        if (!isDdl) {
            //过期时间
            if ("INSERT".equals(type)) {
                //新增语句
                log.info("Insert语句 {}", canalBean);
            } else if ("UPDATE".equals(type)) {
                //更新语句
                log.info("UPDATE语句 {}", canalBean);
            } else {
                log.info("其他语句 {}", canalBean);
            }
        }
    }
}
