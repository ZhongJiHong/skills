package com.bonc.kafka110.producer;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

/**
 * created by G.Goe on 2018/6/4
 * <p>
 * 幂等生产者demo
 */
public class IdempotentProducerDemo {

    private KafkaProducer<byte[], byte[]> kafkaProducer;

    public IdempotentProducerDemo(String bootstrap) {

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);

        props.put("enable.idempotence", "true");
        // props.put("acks", "all");
        // props.put("retries", 0);

        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        this.kafkaProducer = new KafkaProducer<>(props);
    }
}
