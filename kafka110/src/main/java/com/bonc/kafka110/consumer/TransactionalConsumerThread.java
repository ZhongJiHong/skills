package com.bonc.kafka110.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * created by G.Goe on 2018/6/5
 */
public class TransactionalConsumerThread implements Callable<Void> {

    private static final Logger logger = LoggerFactory.getLogger(TransactionalConsumerThread.class);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private KafkaConsumer<byte[], byte[]> kafkaConsumer;
    private String topic;
    private int partition;

    @Override
    public Void call() throws Exception {

        TopicPartition topicPartition = new TopicPartition(topic, partition);
        kafkaConsumer.assign(Collections.singletonList(topicPartition));
        // 从分区的第一个offset开始读取
        kafkaConsumer.seekToBeginning(Collections.singletonList(topicPartition));

        try {
            while (!closed.get()) {

                // 拉取消息
                ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(500);
                // 处理消息
                for (ConsumerRecord<byte[], byte[]> record :
                        records) {
                    // Handle new records
                    // 格式化打印到控制台
                        /*System.out.printf("CurrentThread:%s,Topic:%s,partition:%d,offsets:%d,key-value:%s",
                                Thread.currentThread().toString(), topicPartition.topic(), topicPartition.partition(),
                                record.offset(), new String(record.key(), "UTF-8") + "=" + new String(record.value(), "UTF-8") + "\r\n");*/

                    logger.info(String.format("CurrentThread:%s,Topic:%s,partition:%d,offsets:%d,key-value:%s",
                            Thread.currentThread().toString(), topicPartition.topic(), topicPartition.partition(),
                            record.offset(), new String(record.key(), "UTF-8") + "=" + new String(record.value(), "UTF-8") + "\r\n"));

                    // Thread.sleep(10);
                }
                // 手动提交偏移量
                kafkaConsumer.commitSync();

            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            if (!closed.get()) throw e;
        } finally {
            kafkaConsumer.close();
        }
        return null;
    }

    public TransactionalConsumerThread(String bootstrap, String groupId, String topic, int partition) {

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("group.id", groupId);
        // 设置只消费提交的message - 两种isolation levels(筛选级别)read_committed,read_uncommitted
        props.put("isolation.level", "read_committed");

        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        this.kafkaConsumer = new KafkaConsumer<>(props);
        this.topic = topic;
        this.partition = partition;
    }
}
