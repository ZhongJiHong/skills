package kerberos.digest;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author G.Goe
 * @Date 2018/10/29
 * @Request
 * @Resource
 */

/**
 * 从kafka拉取数据，然后输出到日志中
 */
public class KafkaConsumerThread implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerThread.class);
    private static final AtomicBoolean closed = new AtomicBoolean(false);

    private KafkaConsumer<byte[], byte[]> consumer;
    private String topic;
    private int partition;
    private long offset;

    public KafkaConsumerThread(KafkaConsumer<byte[], byte[]> consumer, String topic, int partition, long offset) {
        this.consumer = consumer;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
    }

    // Shutdown hook which can be called from a separate thread
    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }

    @Override
    public void run() {

        TopicPartition topicPartition = new TopicPartition(topic, partition);
        consumer.assign(Collections.singletonList(topicPartition));
        // consumer.seekToBeginning(Collections.singletonList(topicPartition));
        consumer.seek(topicPartition, offset);
        try {
            while (!closed.get()) {

                ConsumerRecords<byte[], byte[]> records = consumer.poll(1000);
                for (ConsumerRecord<byte[], byte[]> record :
                        records) {

                    String format = String.format("CurrentThread:%s,Topic:%s,partition:%d,offsets:%d,key-value:%s",
                            Thread.currentThread(), record.topic(), record.partition(),
                            record.offset(), Bytes.toInt(record.key()) + "=" + Bytes.toString(record.value()));
                    logger.info(format);
                }
            }
            consumer.commitSync();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            if (!closed.get()) throw e;
        } finally {
            consumer.close();
        }
    }
}
