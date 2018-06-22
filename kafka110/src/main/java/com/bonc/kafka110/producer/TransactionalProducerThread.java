package com.bonc.kafka110.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * created by G.Goe on 2018/6/4
 * <p>
 * 事务生产者demo
 */
public class TransactionalProducerThread implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(TransactionalProducerThread.class);

    private KafkaProducer<byte[], byte[]> kafkaProducer;
    private String topic;
    private int partition;
    private long recordNum;

    @Override
    public void run() {

        // Needs to be called before any other methods when the transactional.id is set in the configuration.
        kafkaProducer.initTransactions();

        try {
            kafkaProducer.beginTransaction();
            for (int i = 0; i < recordNum; i++) {

                kafkaProducer.send(new ProducerRecord<>(topic, partition, (partition + "").getBytes(), ("Transactional Producer测试，这是第" + (i + 1) + "条记录!").getBytes()));
                /*if (i == 5) {
                    throw new KafkaException();
                }*/
            }

            kafkaProducer.commitTransaction();
        } catch (ProducerFencedException | AuthorizationException e) {
            // If the message format of the destination topic is not upgraded to 0.11.0.0, idempotent and transactional produce requests will fail with an UnsupportedForMessageFormatException error.
            // If this is encountered during a transaction, it is possible to abort and continue. But note that future sends to the same topic will continue receiving the same exception until the topic is upgraded.

            // We can't recover from these exceptions, so our only option is to close the producer and exit.
            kafkaProducer.close();
            logger.error(e.getMessage(), e);

        } catch (OutOfOrderSequenceException e) {
            logger.error("Occur OutOfOrderSequenceException!");
            logger.error(e.getMessage(), e);
            kafkaProducer.send(new ProducerRecord<>(topic, partition, (partition + "").getBytes(), "Transactional Producer测试,Occur OutOfOrderSequenceException".getBytes()));
        } catch (KafkaException e) {
            // For all other exceptions, just abort the transaction and try again.
            kafkaProducer.abortTransaction();
            logger.error(e.getMessage(), e);
        }
        kafkaProducer.close();
    }

    /**
     * 线程类的构造函数
     *
     * @param bootstrap
     * @param transactionalId
     * @param topic
     * @param partition
     * @param recordNum       - 一个事务中record的数量
     */
    public TransactionalProducerThread(String bootstrap, String transactionalId, String clientId, String topic, int partition, long recordNum) {

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("client.id", clientId);

        props.put("transactional.id", transactionalId);

        // props.put("enable.idempotence", true);
        // props.put("enable.idempotence", "true");
        // props.put("acks", "all");
        // props.put("retries", 0);

        // props.put("batch.size", 3);
        // props.put("linger.ms", 1);
        // props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        this.kafkaProducer = new KafkaProducer<>(props);
        this.topic = topic;
        this.partition = partition;
        this.recordNum = recordNum;
    }
}
