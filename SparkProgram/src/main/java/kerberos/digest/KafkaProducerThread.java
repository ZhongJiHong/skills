package kerberos.digest;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author G.Goe
 * @Date 2018/10/29
 * @Request
 * @Resource
 */

/**
 * 从控制台获取数据,然后发送到kafka
 */
public class KafkaProducerThread implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerThread.class);
    private static final AtomicBoolean closed = new AtomicBoolean(false);

    private KafkaProducer<byte[], byte[]> producer;
    private String topic;
    private int partition;

    public KafkaProducerThread(KafkaProducer<byte[], byte[]> producer, String topic, int partition) {
        this.producer = producer;
        this.partition = partition;
        this.topic = topic;

        // 增加结束标志
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                logger.info("Exit!");
                closed.set(true);
            }
        });
    }

    @Override
    public void run() {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        while (!closed.get()) {
            try {
                String record = reader.readLine();
               /* producer.send(new ProducerRecord<>(topic, partition, Bytes.toBytes(partition), Bytes.toBytes(record)), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (null != e) {
                            logger.error("Failed record ：{}", metadata.offset());
                            logger.error(e.getMessage(), e);
                        }
                    }
                });*/
                producer.send(new ProducerRecord<>(topic, partition, Bytes.toBytes(partition), Bytes.toBytes(record)), ((metadata, exception) -> {
                    if (null != exception) {
                        logger.error("Failed record ：{}", metadata.offset());
                        logger.error(exception.getMessage(), exception);
                    }
                }));
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }

        // 释放资源
        try {
            reader.close();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }
}
