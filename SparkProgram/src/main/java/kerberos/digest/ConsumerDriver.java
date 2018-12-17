package kerberos.digest;

import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * @Author G.Goe
 * @Date 2018/10/29
 * @Request
 * @Resource
 */

/**
 * 注意权限，授权
 */
public class ConsumerDriver {

    public static void main(String[] args) {

        String bootstrap = "172.16.40.33:9092,172.16.40.34:9092,172.16.40.35:9092";   // 31虚拟机
//        String bootstrap = "172.16.13.31:9092,172.16.13.32:9092,172.16.13.33:9092";   // 31物理机
//        String bootstrap = "172.16.40.41:9092,172.16.40.42:9092,172.16.40.43:9092";   // 41虚拟机
        String groupId = "consumer_group_01";
        String clientId = "consumer_client_01";
        String topic = "zjh0102";
        int partition = 0;
        long offset = 0L;

        KafkaConsumer<byte[], byte[]> consumer = ConsumerFactory.getConsumer(
                bootstrap, groupId, clientId);
        new Thread(new KafkaConsumerThread(consumer, topic, partition, offset)).start();
    }
}
