package kerberos.digest;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @Author G.Goe
 * @Date 2018/12/7
 * @Request
 * @Resource
 */
public class GetOffsetToolTest {

    @Test
    public void printOffset() {

        String bootstrap = "172.16.40.33:9092,172.16.40.34:9092,172.16.40.35:9092";
        String groupId = "getOffsetTool";
        String clientId = "single";

        String topic01 = "zjh0101";
        String topic02 = "zjh0102";
        KafkaConsumer<byte[], byte[]> consumer = ConsumerFactory.getConsumer(bootstrap, groupId, clientId);
        GetOffsetTool.printOffset(consumer, topic01, topic02);
    }
}