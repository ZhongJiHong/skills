package com.bonc.ldc.kafka090.consumer;

import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;

/**
 * Created by yzp on 2018/6/20.
 */
public class SimpleMovingAvgNewConsumer {

    private Properties kafkaProps = new Properties();
    // private String waitTime;
    private KafkaConsumer<String, String> consumer;
    private static final Logger logger = LoggerFactory.getLogger(SimpleMovingAvgNewConsumer.class);

    public static void main(String[] args) {
//        if (args.length == 0) {
//            System.out.println("SimpleMovingAvgZkConsumer {brokers} {group.id} {topic} {window-size}");
//            return;
//        }

        final SimpleMovingAvgNewConsumer movingAvg = new SimpleMovingAvgNewConsumer();
//        String brokers = args[0];
        String brokers = "kafka01:9093,kafka02:9093,kafka03:9093";
//        String groupId = args[1];
        String groupId = "kafka2hbase3";
//        String topic = args[2];
        String topic = "cbss002";

//        int window = Integer.parseInt(args[3]);
        int window = Integer.parseInt("3");

        CircularFifoBuffer buffer = new CircularFifoBuffer(window);
        movingAvg.configure(brokers, groupId);
        final Thread mainThread = Thread.currentThread();

        // Registering a shutdown hook so we can exit cleanly
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("Starting exit...");
                // Note that shutdownhook runs in a separate thread, so the only thing we can safely do to a consumer is wake it up
                movingAvg.consumer.wakeup();
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            movingAvg.consumer.subscribe(Collections.singletonList(topic));

            // looping until ctrl-c, the shutdown hook will cleanup on exit
            while (true) {
                ConsumerRecords<String, String> records = movingAvg.consumer.poll(1000);
                System.out.println(System.currentTimeMillis() + "  --  waiting for data...");
                for (ConsumerRecord<String, String> record : records) {
                    logger.info(record.value());
                    int sum = 0;
                    try {
                        int num = Integer.parseInt(record.value());
                        buffer.add(num);
                    } catch (NumberFormatException e) {
                        // just ignore strings
                    }

                    for (Object o : buffer) {
                        sum += (Integer) o;
                    }

                    if (buffer.size() > 0) {
                        System.out.println("Moving avg is: " + (sum / buffer.size()));
                    }
                }
                for (TopicPartition tp : movingAvg.consumer.assignment())
                    System.out.println("Committing offset at position:" + movingAvg.consumer.position(tp));
                movingAvg.consumer.commitSync();
            }
        } catch (WakeupException e) {
            // ignore for shutdown
        } finally {
            movingAvg.consumer.close();
            System.out.println("Closed consumer and we are done");
        }
    }

    private void configure(String servers, String groupId) {
        kafkaProps.put("group.id", groupId);
        kafkaProps.put("bootstrap.servers", servers);
        kafkaProps.put("auto.offset.reset", "earliest");         // when in doubt, read everything
        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<String, String>(kafkaProps);
    }

}
