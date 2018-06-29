package com.bonc.ldc.kafka090.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * created by G.Goe on 2018/5/4
 */
public class ConsumerThread implements Runnable {

    private final Logger logger = Logger.getLogger(ConsumerThread.class);

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private KafkaConsumer<byte[], byte[]> consumer;
    private TopicPartition topicPartition;
    private Long offsets;

    @Override
    public void run() {

        consumer.assign(Collections.singletonList(topicPartition));
        consumer.seek(topicPartition, offsets);
        try {
            while (!closed.get()) {

                // 拉取消息
                ConsumerRecords<byte[], byte[]> records = consumer.poll(500);
                // 处理消息
                for (ConsumerRecord<byte[], byte[]> record :
                        records) {
                    // Handle new records
                    /*System.out.println("CurrentThread:" + Thread.currentThread() + "\t|\t" + "Topic:" + topicPartition.topic() + "\t|\t"
                            + "Partition:" + topicPartition.partition() + "\t|\t" + "Offsets:" + record.offset() + "\t|\t"
                            + "key-value:" + "(" + new String(record.key(), "UTF-8") + "," + new String(record.value(), "UTF-8") + ")");*/
                    /*System.out.printf("CurrentThread:%s,Topic:%s,partition:%d,offsets:%d,key-value:%s",
                            Thread.currentThread().toString(), topicPartition.topic(), topicPartition.partition(),
                            record.offset(), new String(record.key(), "UTF-8") + "=" + new String(record.value(), "UTF-8") + "\r\n");*/
                    logger.info("CurrentThread:" + Thread.currentThread() + "\t|\t" + "Topic:" + topicPartition.topic() + "\t|\t"
                            + "Partition:" + topicPartition.partition() + "\t|\t" + "Offsets:" + record.offset() + "\t|\t"
                            + "key-value:" + "(" + new String(record.key(), "UTF-8") + "," + new String(record.value(), "UTF-8") + ")");

                }
                // 手动提交偏移量
                // consumer.commitSync();
                Thread.sleep(10);
            }
        } catch (Exception e) {
            logger.error(e.getMessage() + "###", e);
            System.out.println("check consumer is closed?");

            // Ignore exception if closing
            // Throw exception if not closing
            if (!closed.get()) {
                System.out.println("Not Closing!");
                try {
                    try {
                        System.out.println("~~~~~~~~");
                        throw e;
                    } catch (UnsupportedEncodingException e1) {
                        e1.printStackTrace();
                    }
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        } finally {
            System.out.println("closing~~");
            consumer.close();
        }

    }

    // Shutdown hook which can be called from a separate thread
    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }

    /**
     * 构造方法 - 指定topic partition offsets
     *
     * @param bootstrap_servers
     * @param group_id
     * @param topic
     * @param partition
     * @param offsets
     */
    public ConsumerThread(String bootstrap_servers, String group_id, String topic, int partition, Long offsets) {
        this.offsets = offsets;

        Properties prop = new Properties();
        prop.put("bootstrap.servers", bootstrap_servers);
        prop.put("group.id", group_id);
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        prop.put("enable.auto.commit", "true");
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(prop);
        this.consumer = consumer;

        TopicPartition topicPartition = new TopicPartition(topic, partition);

        this.topicPartition = topicPartition;
    }
}
