package producer;

import com.bonc.kafka110.producer.FileProducer;
import org.junit.Test;

/**
 * created by G.Goe on 2018/6/1
 */
public class FileProducerTest {

    @Test
    public void produceData() {

        FileProducer producer = new FileProducer("kafka03:19093");
        // partition 0
        producer.produceData("D:\\tf_f_user_item.txt", "test", 0);
    }
}