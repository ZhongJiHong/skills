package com.bonc.kafka110.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * created by G.Goe on 2018/6/1
 */
public class NumProducerThread implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(RecordsProducerThread.class);

    private KafkaProducer<byte[], byte[]> kafkaProducer;

    private String topic;
    private int partition;
    private long recordNum;

    /**
     * 异步发送数据，并设置回调函数
     */
    @Override
    public void run() {

        int count = 0;
        while (count < recordNum) {

            /*kafkaProducer.send(new ProducerRecord<>(topic, partition, ("" + partition).getBytes(), ("key是分区号，value是写死的，就像这样!").getBytes()),
                    new CustomCallback());*/ // 发送 - 传入回调对象
            kafkaProducer.send(new ProducerRecord<>(topic, partition,
                            ("" + partition).getBytes(), ("key是分区号，value是写死的，就像这样!").getBytes()),
                    new CustomCallback());

            count++;
        }
        kafkaProducer.close();
        logger.info("{} records has been send to Kafka !", recordNum);
    }

    /**
     * 异步处理回调函数
     */
    private class CustomCallback implements Callback {

        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {

            // 此处的处理有待考量
            if (e != null) {
                logger.error("Failed record ：{}", recordMetadata.offset());
                logger.error(e.getMessage(), e);
            } else {
                logger.info("The offset of the record we just sent is: {}", recordMetadata.offset());
            }
        }
    }

    /**
     * 线程类构造函数 - 指定topic,partition，并指定数据量
     *
     * @param bootstrap - Kafka集群
     * @param clientId  - 客户端id
     * @param topic     - 主题
     * @param partition - 分区
     * @param recordNum - 发送的数据量
     */
    public NumProducerThread(String bootstrap, String clientId, String topic, int partition, long recordNum) {

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("client.id", clientId);

        // 设置确认超时时间,默认是30000
        props.put("request.timeout.ms", 12000);

        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 1000);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        this.kafkaProducer = new KafkaProducer<>(props);
        this.recordNum = recordNum;
        this.topic = topic;
        this.partition = partition;
    }
}
