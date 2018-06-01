package com.bonc.kafka110.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * created by G.Goe on 2018/6/1
 */
public class FileProducer {

    private static Logger logger = LoggerFactory.getLogger(FileProducer.class);

    private KafkaProducer<byte[], byte[]> kafkaProducer;

    public void produceData(String file_path, String topic, int partition) {

        File file = new File(file_path);
        FileInputStream input = null;

        try {
            input = new FileInputStream(file);
            byte[] buffer = new byte[(int) file.length()];

            // 将数据读取到内存中
            int len = input.read(buffer);

            String data = new String(buffer, 0, len);
            String[] records = data.split("\\r");

            for (String record :
                    records) {
                kafkaProducer.send(
                        new ProducerRecord<byte[], byte[]>(
                                topic, partition, (partition + "").getBytes("UTF-8"), record.getBytes("UTF-8"))).get();
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), "数据上传失败！");
        } finally {
            try {
                if (null != input)
                    input.close();
            } catch (IOException e) {
                logger.error(e.getMessage(), "流关闭异常！");
            } finally {
                input = null;
            }
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

        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props);
        this.kafkaProducer = producer;
    }
}
