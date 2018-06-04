package com.bonc.kafka110.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

/**
 * created by G.Goe on 2018/6/1
 * <p>
 * 这是一个Demo
 */
public class FileProducerDemo {

    private static Logger logger = LoggerFactory.getLogger(FileProducerDemo.class);

    private KafkaProducer<byte[], byte[]> kafkaProducer;

    public void produceData(String filePath, String topic, int partition) {

        File file = new File(filePath);
        byte[] buffer = null;

        // 文件数据的解析规则
        try (
                FileInputStream input = new FileInputStream(file)
        ) {
            buffer = new byte[(int) file.length()];

            // 将数据读取到内存中
            input.read(buffer);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

        String data = new String(buffer);
        String[] records = data.split("\\r");

        // 发送解析的数据
        for (String record :
                records) {
            kafkaProducer.send(
                    new ProducerRecord<>(
                            topic, partition, (partition + "").getBytes(), record.getBytes()));
        }

        // 此方法是阻塞的
        kafkaProducer.close();
        logger.info("{}s records has been send to Kafka!", records.length);
    }

    /**
     * @param bootstrap - Kafka服务节点
     */
    public FileProducerDemo(String bootstrap) {

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        this.kafkaProducer = new KafkaProducer<>(props);
    }
}
