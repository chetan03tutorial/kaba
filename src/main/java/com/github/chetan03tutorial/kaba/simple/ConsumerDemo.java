package com.github.chetan03tutorial.kaba.simple;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);
    public static void main(String[] args) {
        System.out.println("Hello World");
        String topicName = "first_topic";
        String bootstrapServer = "localhost:9092";
        String consumerGroup = "kaba-cg1";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,consumerGroup);

        // create consumer
        Consumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        // subscribe to producer
        kafkaConsumer.subscribe(Collections.singleton(topicName));

        // poll for new data
        while(true){
            ConsumerRecords<String,String> consumerRecord = kafkaConsumer.poll(Duration.ofMillis(100));
            consumerRecord.forEach((record) -> {
                logger.info(record.key());
                logger.info(record.value());
            } );
        }
    }
}
