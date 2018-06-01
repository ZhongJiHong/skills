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
 */
public class FileProducer {

    private static Logger logger = LoggerFactory.getLogger(FileProducer.class);

    private KafkaProducer<byte[], byte[]> kafkaProducer;

    public void produceData(String filePath, String topic, int partition) {

        File file = new File(filePath);

        try (
                FileInputStream input = new FileInputStream(file)
        ) {
            byte[] buffer = new byte[(int) file.length()];

            // 将数据读取到内存中
            int len = input.read(buffer);

            String data = new String(buffer, 0, len);
            String[] records = data.split("\\r");

            for (String record :
                    records) {
                kafkaProducer.send(
                        new ProducerRecord<>(
                                topic, partition, (partition + "").getBytes("UTF-8"), record.getBytes("UTF-8"))).get();
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    public FileProducer(String bootstrap) {

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
