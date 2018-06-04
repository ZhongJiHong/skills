package com.bonc.kafka110.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * created by G.Goe on 2018/6/1
 */
public class RecordsProducerThread implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(RecordsProducerThread.class);

    private KafkaProducer<byte[], byte[]> kafkaProducer;

    private String topic;
    private int partition;
    private String[] records;

    /**
     * 异步发送数据，并设置回调函数
     */
    @Override
    public void run() {

        // 发送解析的数据
        for (String record :
                records) {
            kafkaProducer.send(
                    new ProducerRecord<>(
                            topic, partition, (partition + "").getBytes(), record.getBytes()),
                    new CustomCallback());
        }

        // 此方法是阻塞的
        kafkaProducer.close();
        logger.info("{}s records has been send to Kafka!", records.length);
    }

    /**
     * 异步处理回调函数
     */
    private class CustomCallback implements Callback {

        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {

            // 此处的处理有待考量
            if (null != e) {
                logger.error("Failed record ：{}", recordMetadata.offset());
                logger.error(e.getMessage(), e);
            }
        }
    }

    /**
     * 线程类构造函数
     *
     * @param bootstrap - Kafka集群
     * @param clientId  - 客户端id
     * @param topic     - 主题
     * @param partition - 分区
     * @param records   - 数据记录
     */
    public RecordsProducerThread(String bootstrap, String clientId, String topic, int partition, String[] records) {

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("client.id", clientId);

        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        this.kafkaProducer = new KafkaProducer<>(props);
        this.records = records;
        this.topic = topic;
        this.partition = partition;
    }
}
