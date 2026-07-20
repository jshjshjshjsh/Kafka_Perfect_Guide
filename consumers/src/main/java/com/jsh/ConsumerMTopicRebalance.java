package com.jsh;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerMTopicRebalance {

    public static final Logger logger = LoggerFactory.getLogger(ConsumerMTopicRebalance.class.getName());

    public static void main(String[] args) {

        //String topicName = "pizza-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-assign");
        //props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());
        props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());


        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(props);
        // collection을 넣을 수 있기 때문에 하나 더 추가
        kafkaConsumer.subscribe(List.of("topic-p3-t1", "topic-p3-t2"));

        // main thread 참조 변수 선언
        Thread mainThread = Thread.currentThread();

        // 최후의 유언을 만들 쓰레드
        // main thread 종료 시 별도의 thread로 kafkaConsumer wakeup() 메소드를 호출하게 함.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("main program starts to exit by calling wakeup");
            kafkaConsumer.wakeup();

            try{
                mainThread.join();
            }catch(InterruptedException e){
                e.printStackTrace();
            }

        }));

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    logger.info("topic:{}, record key:{}, partition:{}, record offset:{}, record value:{}",
                            record.topic(), record.key(), record.partition(), record.offset(), record.value());
                }
            }
        } catch (Exception e) {
            logger.error("wakeup exception has been called");
        } finally{
            logger.info("finally consumer is closing");
            kafkaConsumer.close();
        }

        /**
         * min@min-VirtualBox:~$ kafka-topics --bootstrap-server localhost:9092 --create --topic topic-p3-t1 --partitions 3
         * min@min-VirtualBox:~$ kafka-topics --bootstrap-server localhost:9092 --create --topic topic-p3-t2 --partitions 3
         * min@min-VirtualBox:~$ kafka-console-producer --bootstrap-server localhost:9092 --topic topic-p3-t1
         * min@min-VirtualBox:~$ kafka-console-producer --bootstrap-server localhost:9092 --topic topic-p3-t2
         * */
    }
}
