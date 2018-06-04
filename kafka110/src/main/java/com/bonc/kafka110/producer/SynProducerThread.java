package com.bonc.kafka110.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Callable;

/**
 * created by G.Goe on 2018/6/4
 */
public class SynProducerThread implements Callable<Void> {

    private static Logger logger = LoggerFactory.getLogger(SynProducerThread.class);

    private KafkaProducer<byte[], byte[]> kafkaProducer;
    private final String topic;
    private final int partition;
    private final long recordNum;

    @Override
    public Void call() throws Exception {

        int count = 0;
        while (count < recordNum) {
            kafkaProducer.send(new ProducerRecord<>(topic, partition, ("" + partition).getBytes(), ("key是分区号，value是写死的，就像这样！").getBytes())).get(); // 发送 - 传入回调对象
            count++;
        }
        kafkaProducer.close();
        logger.info("{} records has been send to Kafka !", recordNum);
        return null;
    }

    public SynProducerThread(String bootstrap, String clientId, String topic, int partition, long recordNum) {

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
        this.topic = topic;
        this.partition = partition;
        this.recordNum = recordNum;
    }

    // TODO
}
