package com.bonc.kafka110.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * created by G.Goe on 2018/6/4
 * <p>
 * 幂等生产者demo
 */
public class IdempotentProducerThread implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(IdempotentProducerThread.class);

    private KafkaProducer<byte[], byte[]> kafkaProducer;
    private String topic;
    private int partition;
    private long recordNum;

    @Override
    public void run() {

        for (long i = 0; i < recordNum; i++) {
            try {
                /*kafkaProducer.send(new ProducerRecord<>(topic, partition, (partition + "").getBytes(), ("Idempotent Producer测试，这是第" + (i + 1) + "条记录!").getBytes()),
                        new CustomCallback());*/
                Future<RecordMetadata> future = kafkaProducer.send(new ProducerRecord<>(topic, partition, (partition + "").getBytes(), ("Idempotent Producer测试，这是第" + (i + 1) + "条记录!").getBytes()));
                // future.get()
            } catch (AuthorizationException e) {
                // it is possible to continue sending after receiving an OutOfOrderSequenceException, but doing so can result in out of order delivery of pending messages.
                // To ensure proper ordering, you should close the producer and create a new instance.
                // We can't recover from these exceptions, so our only option is to close the producer and exit.
                kafkaProducer.close();
                logger.error(e.getMessage(), e);
            } catch (OutOfOrderSequenceException e) {
                logger.error(e.getMessage(), e);
                logger.error("The record that Occur exception is {}", (i + 1));
                kafkaProducer.send(new ProducerRecord<>(topic, partition, (partition + "").getBytes(), ("Idempotent Producer测试，Occur OutOfOrderSequenceException!").getBytes()));
            } catch (KafkaException e) {
                // For all other exceptions, just abort the transaction and try again.
                logger.error(e.getMessage(), e);
            }
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
     * 线程类构造函数
     *
     * @param bootstrap
     * @param clientId
     * @param topic
     * @param partition
     * @param recordNum
     */
    public IdempotentProducerThread(String bootstrap, String clientId, String topic, int partition, long recordNum) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("client.id", clientId);

        // 启用幂等生产者，会自动设置"acks"为"all","retries"为Integer.MAX_VALUE,不必再设置
        props.put("enable.idempotence", true);
        // props.put("acks", "all");
        // props.put("retries", 0);

        // 设置请求超时时间，默认是30000，即30秒
        // props.put("request.timeout.ms", 12000);

        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        this.kafkaProducer = new KafkaProducer<>(props);
        this.topic = topic;
        this.partition = partition;
        this.recordNum = recordNum;
    }
}
