package com.jsh.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleProducerAsync {

    public static final Logger logger = LoggerFactory.getLogger(SimpleProducerAsync.class.getName());

    public static void main(String[] args) {

        String topicName = "simple-topic";

        // kafkaProducer config 설정
        Properties props = new Properties();
        // 둘이 같음
        props.setProperty("bootstrap.servers", "192.168.56.101:9092");
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // kafkaProducer 객체 생성
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);

        // ProducerRecord 객체 생성
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topicName, "id-001", "hello world async");

        // KafkaProducer 매세지 발송
        kafkaProducer.send(producerRecord, (recordMetadata, exception) -> {
            if (exception == null) {
                logger.info("\n ###### record metadata received ###### \n" +
                        "partition: " + recordMetadata.partition() + "\n" +
                        "offset: " + recordMetadata.offset() + "\n" +
                        "timestamp: " + recordMetadata.timestamp() + "\n");

            } else {
                logger.error("exception error from broker " + exception.getMessage());

            }

        });

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        kafkaProducer.close();


    }
}
