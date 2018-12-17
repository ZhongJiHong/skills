package kerberos.digest;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @Author G.Goe
 * @Date 2018/12/7
 * @Request JDK-1.8及以上版本
 * @Resource
 */
public class GetOffsetTool {

    private GetOffsetTool() {
    }

    private static final Logger logger = LoggerFactory.getLogger(GetOffsetTool.class);


    // 以后可以添加更多的重载方法
    public static void printOffset(KafkaConsumer consumer, String... topics) {

        List<TopicPartition> topicPartitions = new LinkedList<>();

        Arrays.asList(topics).forEach(topic -> {
            List<PartitionInfo> list = consumer.partitionsFor(topic);
            list.forEach(info -> {
                topicPartitions.add(new TopicPartition(info.topic(), info.partition()));
            });
        });

        // 搞定
        Map<TopicPartition, Long> map = consumer.endOffsets(topicPartitions);
        map.keySet().stream().sorted((head, tail) -> {  // 二次排序
            if (head.topic().equals(tail.topic())) {
                return head.partition() - tail.partition();
            } else {
                return head.topic().compareTo(tail.topic());
            }
        }).forEach(element -> { // 输出
            String error = String.format("topic:%s,partition:%d,offset:%d", element.topic(), element.partition(), map.get(element));
            logger.error(error);
        });
    }
}
