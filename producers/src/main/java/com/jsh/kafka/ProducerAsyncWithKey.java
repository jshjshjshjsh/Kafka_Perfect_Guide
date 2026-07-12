package com.jsh.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerAsyncWithKey {

    public static final Logger logger = LoggerFactory.getLogger(ProducerAsyncWithKey.class.getName());

    public static void main(String[] args) {

        String topicName = "multipart-topic";

        // kafkaProducer config 설정
        Properties props = new Properties();
        // 둘이 같음
        props.setProperty("bootstrap.servers", "192.168.56.101:9092");
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // kafkaProducer 객체 생성
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);

        for(int seq=0; seq<10; seq++) {

            // ProducerRecord 객체 생성
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topicName, String.valueOf(seq), "hello world " + seq);

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
        }

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        kafkaProducer.close();


        /**
         *  delete multipart-topic
         *  create multipart-topic partition 3
         *  kafka-console-consumer --bootstrap-server localhost:9092 --group group-01 --topic multipart-topic \
         * --property print.key=true --property print.value=true
         * */
    }
}
