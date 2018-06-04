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
 * created by G.Goe on 2018/6/1
 */
public class LoggerConsumerThread implements Callable<Void> {

    private static Logger logger = LoggerFactory.getLogger(LoggerConsumerThread.class);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private static long count = 0;

    private KafkaConsumer<byte[], byte[]> kafkaConsumer;
    private String topic;
    private int partition;
    private long beginningOffset;

    @Override
    public Void call() throws Exception {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        kafkaConsumer.assign(Collections.singletonList(topicPartition));
        kafkaConsumer.seek(topicPartition, beginningOffset);

        try {
            while (!closed.get()) {

                // if (count < 1999999) {  /*Kafka消费测试*/
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
                    count++;
                }
                // 手动提交偏移量
                kafkaConsumer.commitSync();
                // } else {
                //     shutdown();
                // }

            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            if (!closed.get()) throw e;
        } finally {
            kafkaConsumer.close();
        }

        return null;
    }

    // Shutdown hook which can be called from a separate thread
    public void shutdown() {
        closed.set(true);
        kafkaConsumer.wakeup();
    }

    /**
     * 消费线程的构造函数
     *
     * @param bootstrap       - Kafka集群
     * @param groupId         - 消费组id
     * @param topic           - 主题
     * @param partition       - 分区号
     * @param beginningOffset - 消费起始偏移量
     */
    public LoggerConsumerThread(String bootstrap, String groupId, String topic, int partition, long beginningOffset) {

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("group.id", groupId);

        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        this.kafkaConsumer = new KafkaConsumer<>(props);
        this.topic = topic;
        this.partition = partition;
        this.beginningOffset = beginningOffset;
    }
}
