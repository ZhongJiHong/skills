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
public class TransactionalProducerDemo {

    private static final Logger logger = LoggerFactory.getLogger(TransactionalProducerDemo.class);

    private KafkaProducer<byte[], byte[]> kafkaProducer;

    public void producerData() {

        // Needs to be called before any other methods when the transactional.id is set in the configuration.
        kafkaProducer.initTransactions();

        try {
            kafkaProducer.beginTransaction();
            for (int i = 0; i < 100; i++)
                kafkaProducer.send(new ProducerRecord<>("goe001", (i + "").getBytes(), "Transactional Producer的测试用例！".getBytes()));
            kafkaProducer.commitTransaction();
        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            // If the message format of the destination topic is not upgraded to 0.11.0.0, idempotent and transactional produce requests will fail with an UnsupportedForMessageFormatException error.
            // If this is encountered during a transaction, it is possible to abort and continue. But note that future sends to the same topic will continue receiving the same exception until the topic is upgraded.

            // We can't recover from these exceptions, so our only option is to close the producer and exit.
            kafkaProducer.close();
            logger.error(e.getMessage(), e);
        } catch (KafkaException e) {
            // For all other exceptions, just abort the transaction and try again.
            kafkaProducer.abortTransaction();
            logger.error(e.getMessage(), e);
        }
        kafkaProducer.close();
    }

    public TransactionalProducerDemo(String bootstrap) {

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);

        props.put("transactional.id", "my-transactional-id");
        // props.put("enable.idempotence", "true");
        // props.put("acks", "all");
        // props.put("retries", 0);

        // props.put("batch.size", 16384);
        // props.put("linger.ms", 1);
        // props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        this.kafkaProducer = new KafkaProducer<>(props);
    }
}
