package com.jsh.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SimpleProducer {
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
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topicName, "id-001", "hello world2");

        // KafkaProducer 매세지 발송
        kafkaProducer.send(producerRecord);

        kafkaProducer.flush();
        kafkaProducer.close();

        /**
         * min@min-VirtualBox:~$ kafka-console-consumer --bootstrap-server localhost:9092 --topic simple-topic --from-beginning         * hello world
         * */
    }
}
