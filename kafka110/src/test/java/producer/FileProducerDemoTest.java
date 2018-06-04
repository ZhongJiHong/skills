package producer;

import com.bonc.kafka110.producer.FileProducerDemo;
import org.junit.Test;

/**
 * created by G.Goe on 2018/6/1
 */
public class FileProducerDemoTest {

    @Test
    public void produceData() {

        FileProducerDemo producer = new FileProducerDemo("kafka03:19093");
        // partition 0
        producer.produceData("D:\\tf_f_user_item.txt", "goe001", 0);
    }
}