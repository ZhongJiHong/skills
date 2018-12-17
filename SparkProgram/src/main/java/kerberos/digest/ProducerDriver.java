package kerberos.digest;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.io.IOException;

/**
 * @Author G.Goe
 * @Date 2018/10/29
 * @Request
 * @Resource
 */

/**
 * 注意权限问题，授权
 */
public class ProducerDriver {

    public static void main(String[] args) throws IOException {

        String bootstrap = "172.16.40.33:9092,172.16.40.34:9092,172.16.40.35:9092";   // 31虚拟机
//        String bootstrap = "172.16.13.31:9092,172.16.13.32:9092,172.16.13.33:9092";   // 31物理机
        // 41虚拟机
//        String bootstrap = "172.16.40.41:9092,172.16.40.42:9092,172.16.40.43:9092";
        String clientId = "producer_client_01";
        String topic = "zjh0102";
        int partition = 0;

        KafkaProducer<byte[], byte[]> producer = ProducerFactory.getProducer(
                bootstrap, clientId);
        new Thread(new KafkaProducerThread(producer, topic, partition)).start();
    }
}
