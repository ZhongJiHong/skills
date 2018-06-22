package com.bonc.kafka110.tryandtry;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class TransactionProducerDemo {
    public static void onlyProduceInTransaction() {
        Producer producer = buildProducer();

        // 1.初始化事务
        producer.initTransactions();

        // 2.开启事务
        producer.beginTransaction();

        try {
            // 3.kafka写操作集合
            // 3.1 do业务逻辑

            // 3.2 发送消息
            for (int i = 1; i <= 100; i++) {
                String value = "Transactional Producer测试，这是第" + (i + 1) + "条记录!";
                producer.send(new ProducerRecord("zhzh", 0, "0".getBytes(), value.getBytes()));
            }


            //producer.send(new ProducerRecord<String, String>("zh-test002", "transaction-data-2"));
            // 3.3 do其他业务逻辑,还可以发送其他topic的消息。

            // 4.事务提交
            producer.commitTransaction();


        } catch (Exception e) {
            // 5.放弃事务
            producer.abortTransaction();
        }

    }

    private static Producer buildProducer() {

        // create instance for properties to access producer configs
        Properties props = new Properties();

        // bootstrap.servers是Kafka集群的IP地址。多个时,使用逗号隔开
        props.put("bootstrap.servers", "172.16.40.116:29092,172.16.40.116:29093,172.16.40.116:29094,172.16.40.116:29095");

        // 设置事务id
        props.put("transactional.id", "tx_025779");

        // 设置幂等性
        props.put("enable.idempotence", true);

        //Set acknowledgements for producer requests.
        // props.put("acks", "all");

        //If the request fails, the producer can automatically retry,
        // props.put("retries", 1);

        //Specify buffer size in config,这里不进行设置这个属性,如果设置了,还需要执行producer.flush()来把缓存中消息发送出去
        //props.put("batch.size", 16384);

        //Reduce the no of requests less than 0
        // props.put("linger.ms", 1);

        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        // props.put("buffer.memory", 33554432);

        // Kafka消息是以键值对的形式发送,需要设置key和value类型序列化器
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.ByteArraySerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.ByteArraySerializer");


        Producer<byte[], byte[]> producer = new KafkaProducer(props);

        return producer;
    }

    public static void main(String[] args) {
        onlyProduceInTransaction();
        // producer.initTransactions();
    }
}
