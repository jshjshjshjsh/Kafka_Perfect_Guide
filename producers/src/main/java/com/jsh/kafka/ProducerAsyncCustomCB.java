package com.jsh.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerAsyncCustomCB {

    public static final Logger logger = LoggerFactory.getLogger(ProducerAsyncCustomCB.class.getName());

    public static void main(String[] args) {

        String topicName = "multipart-topic";

        // kafkaProducer config 설정
        Properties props = new Properties();
        // 둘이 같음
        props.setProperty("bootstrap.servers", "192.168.56.101:9092");
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // kafkaProducer 객체 생성
        KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<Integer, String>(props);

        for(int seq=0; seq<10; seq++) {

            // ProducerRecord 객체 생성
            ProducerRecord<Integer, String> producerRecord = new ProducerRecord<Integer, String>(topicName, seq, "hello world " + seq);

            Callback callback = new CustomCallback(seq) {};

            // KafkaProducer 매세지 발송
            kafkaProducer.send(producerRecord, callback);
        }

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        kafkaProducer.close();


        /**
         * 출력할때 key를 integer로 바꿔서 출력해줘야 함.
         *
         * kafka-console-consumer --bootstrap-server localhost:9092 --group group-01 --topic multipart-topic \
         * --property print.key=true --property print.value=true \
         * --key-deserializer "org.apache.kafka.common.serialization.IntegerDeserializer"
         * */
    }
}
