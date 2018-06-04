package producer;

import com.bonc.kafka110.producer.ProducerThread;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * created by G.Goe on 2018/6/1
 */
public class ProducerThreadTest {

    private String bootstrap = "172.16.40.116:19093";
    private int nThread = 3;
    private String topic = "goe002";

    @Test
    public void run() throws InterruptedException {

        ExecutorService pool = Executors.newFixedThreadPool(nThread);
        pool.submit(new ProducerThread(bootstrap, "client001", topic, 0, 1000));
        pool.submit(new ProducerThread(bootstrap, "client002", topic, 1, 1000));
        pool.submit(new ProducerThread(bootstrap, "client003", topic, 2, 1000));

        pool.shutdown();
        pool.awaitTermination(600, TimeUnit.SECONDS);
    }
}