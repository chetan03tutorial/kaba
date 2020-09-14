package com.github.chetan03tutorial.kaba.simple;

import com.pdms.person.PersonOuterClass;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    final static Logger logger = LoggerFactory.getLogger(ProducerDemo.class);
    public static void main(String[] args) {

        String bootStrapServer = "localhost:9092";
        // create producer properties
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServer);
        kafkaProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());


        // create producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(kafkaProperties);

        // Create a producer record
        final ProducerRecord<String,String> record = new ProducerRecord<String, String>("first_topic", "key1","Hello World Third time");
        // send message

        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                logger.info(" Topic : " + recordMetadata.topic());
            }
        });
        // flush data
        producer.flush();
        // flush and close producer
        producer.close();
    }
}
