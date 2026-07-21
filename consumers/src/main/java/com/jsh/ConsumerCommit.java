package com.jsh;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ConsumerCommit {

    public static final Logger logger = LoggerFactory.getLogger(ConsumerCommit.class.getName());

    public static void main(String[] args) {

        String topicName = "pizza-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-03");
        //props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "6000");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");


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

        //pollAutoCommit(kafkaConsumer);
        //pollCommitSync(kafkaConsumer);
        pollCommitAsync(kafkaConsumer);
        

        /**
         *
         * */
    }

    private static void pollCommitAsync(KafkaConsumer<String, String> kafkaConsumer) {
        int loopCnt = 0;

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                logger.info("###### loopCnt: {} consumerRecords count: {}", loopCnt++, consumerRecords.count());


                for (ConsumerRecord<String, String> record : consumerRecords) {
                    logger.info("record key:{}, partition:{}, record offset:{}, record value:{}",
                            record.key(), record.partition(), record.offset(), record.value());
                }

                // 성능적으로 비동기가 당연히 유리함
                kafkaConsumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                        if (exception != null) {
                            logger.error("offsets {} is not completed, error:{}", offsets, exception);

                        }
                    }
                });


            }
        } catch (WakeupException e) {
            logger.error("wakeup exception has been called");
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally{
            // 비동기로 sync를 하면 날아가는게 있을 수 있으므로 동기적으로 마지막 commit
            logger.info("#### commit sync before closing");
            kafkaConsumer.commitSync();
            logger.info("finally consumer is closing");
            kafkaConsumer.close();
        }

    }

    private static void pollCommitSync(KafkaConsumer<String, String> kafkaConsumer) {
        int loopCnt = 0;

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                logger.info("###### loopCnt: {} consumerRecords count: {}", loopCnt++, consumerRecords.count());


                for (ConsumerRecord<String, String> record : consumerRecords) {
                    logger.info("record key:{}, partition:{}, record offset:{}, record value:{}",
                            record.key(), record.partition(), record.offset(), record.value());
                }

                try {
                    if (consumerRecords.count() > 0) {
                        kafkaConsumer.commitSync();
                        logger.info("commit sync has been called");
                    }

                } catch (CommitFailedException e) {
                    logger.error(e.getMessage());
                }


            }
        } catch (WakeupException e) {
            logger.error("wakeup exception has been called");
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally{
            logger.info("finally consumer is closing");
            kafkaConsumer.close();
        }
    }

    public static void pollAutoCommit(KafkaConsumer<String, String> kafkaConsumer) {
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
                    logger.info("main thread is sleeping {} ms during while loop", 10000);
                    Thread.sleep(10000);
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
    }
}
