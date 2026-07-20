package com.jsh;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerWakeupV2 {

    public static final Logger logger = LoggerFactory.getLogger(ConsumerWakeupV2.class.getName());

    public static void main(String[] args) {

        String topicName = "pizza-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-02");
        props.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "60000");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(props);
        kafkaConsumer.subscribe(List.of(topicName));

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

        int loopCnt = 0;

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                logger.info("###### loopCnt: {} consumerRecords count: {}", loopCnt++, consumerRecords.count());


                for (ConsumerRecord<String, String> record : consumerRecords) {
                    logger.info("record key:{}, partition:{}, record offset:{}, record value:{}",
                            record.key(), record.partition(), record.offset(), record.value());
                }

                try{
                    logger.info("main thread is sleeping {} ms during while loop", loopCnt*10000);
                    Thread.sleep(loopCnt*10000);
                }catch(InterruptedException e){
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            logger.error("wakeup exception has been called");
        } finally{
            logger.info("finally consumer is closing");
            kafkaConsumer.close();
        }

        /**
         * 1. consumer.config용 config 파일을 생성.
         * echo "exclude.internal.topics=false" > consumer_temp.config
         *
         * 2. __consumer_offsets 토픽을 읽기
         *  kafka-console-consumer --consumer.config /home/min/consumer_temp.config \
         *  --bootstrap-server localhost:9092 --topic __consumer_offsets \
         *  --formatter "kafka.coordinator.group.GroupMetadataManager\$OffsetsMessageFormatter"
         * */
    }
}
