package producer;

import com.bonc.kafka110.producer.TransactionalProducerDemo;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * created by G.Goe on 2018/6/5
 */
public class TransactionalProducerDemoTest {

    @Test
    public void producerData() {

        long start = System.currentTimeMillis();
        String bootstrap = "172.16.40.116:19093";
        TransactionalProducerDemo producer = new TransactionalProducerDemo(bootstrap);
        producer.producerData();
        long end = System.currentTimeMillis();
        System.out.println("生产耗时：" + (end - start));
    }
}