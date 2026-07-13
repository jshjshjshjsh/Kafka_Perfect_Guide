package com.jsh.kafka;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class PizzaProducer {

    public static final Logger logger = LoggerFactory.getLogger(PizzaProducer.class.getName());

    public static void sendPizzaMessage(KafkaProducer<String, String> kafkaProducer,
                                        String topicName, int iterCount,
                                        int interIntervalMillis, int interverMillis,
                                        int intervalCount, boolean sync){
        PizzaMessage pizzaMessage = new PizzaMessage();

        int iterSeq = 0;

        // seed값을 고정하여 Random 객체와 Faker 객체를 생성.
        long seed = 2022;
        Random random = new Random(seed);
        Faker faker = Faker.instance(random);

        while(iterSeq++ != iterCount){
            HashMap<String, String> pMessage = pizzaMessage.produce_msg(faker, random, iterSeq);
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName,
                    pMessage.get("key"), pMessage.get("message"));

            sendMessage(kafkaProducer, producerRecord, pMessage, sync);

            if(intervalCount > 0 && (iterSeq % intervalCount == 0)){
                try {
                    logger.info("###### IntervalCount:" + intervalCount +
                            " intervalMillis:" + interverMillis + " ######");
                    Thread.sleep(interverMillis);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }

            if (interIntervalMillis > 0) {
                try {
                    logger.info("interIntervalMillis:" + interIntervalMillis);
                    Thread.sleep(interIntervalMillis);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }

        }
    }

    public static void sendMessage(KafkaProducer<String, String> kafkaProducer,
                                   ProducerRecord<String, String> producerRecord,
                                   HashMap<String, String> pMessage, boolean sync){

        if(!sync) {
            // KafkaProducer 매세지 발송
            kafkaProducer.send(producerRecord, (metadata, exception) -> {
                if (exception == null) {
                    logger.info("async message: " + pMessage.get("key") +
                            "partition: " + metadata.partition() +
                            "offset: " + metadata.offset());

                } else {
                    logger.error("exception error from broker " + exception.getMessage());

                }

            });
        } else{
            try {
                RecordMetadata metadata = kafkaProducer.send(producerRecord).get();
                logger.info("sync message: " + pMessage.get("key") +
                        "partition: " + metadata.partition() +
                        "offset: " + metadata.offset());


            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void main(String[] args) {

        String topicName = "pizza-topic";

        // kafkaProducer config 설정
        Properties props = new Properties();
        // 둘이 같음
        props.setProperty("bootstrap.servers", "192.168.56.101:9092");
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // acks 설정
        //props.setProperty(ProducerConfig.ACKS_CONFIG, "0");

        // batch 설정
        //props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "32000");
        //props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");

        // 이게 비정상적으로 짧아지면 에러를 터뜨림
        // Exception in thread "main" org.apache.kafka.common.KafkaException: Failed to construct kafka producer
        //props.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "29000");
        props.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "50000");

        // 만약 전송 시작하고 있는데 kafka가 죽었다고 가정하면 Timeout 에러가 터짐
        // Exception in thread "main" java.lang.RuntimeException: java.util.concurrent.ExecutionException: org.apache.kafka.common.errors.TimeoutException: Expiring 1 record(s) for pizza-topic-1:50001 ms has passed since batch creation

        // 아래 2개만 하면 idempotence가 적용되지 않음
        // 그리고 그냥 기동도 잘 됨
        //props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "6");
        //props.setProperty(ProducerConfig.ACKS_CONFIG, "0");
        // 근데 명시적으로 넣으면 작동 안됨(정상)
        // 잘못된 config를 넣었기 때문에 idempotence도 동작 안하고 에러를 띄우는 거임
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");


        // kafkaProducer 객체 생성
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);

        sendPizzaMessage(kafkaProducer, topicName,
                -1, 10, 100, 100, true);

        kafkaProducer.close();


        /**
         *  min@min-VirtualBox:~$ kafka-topics --bootstrap-server localhost:9092 --create --topic pizza-topic --partitions 3
         * */
    }
}
