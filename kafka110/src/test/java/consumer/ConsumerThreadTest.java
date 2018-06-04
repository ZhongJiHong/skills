package consumer;

import com.bonc.kafka110.consumer.ConsumerThread;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * created by G.Goe on 2018/6/1
 */
public class ConsumerThreadTest {

    private String bootstrap = "172.16.40.116:19093";

    @Test
    public void call() throws InterruptedException {

        ExecutorService pool = Executors.newCachedThreadPool();
        pool.submit(new ConsumerThread(bootstrap, "group001", "goe002", 0, 900L));
        pool.submit(new ConsumerThread(bootstrap, "group002", "goe002", 1, 900L));
        pool.submit(new ConsumerThread(bootstrap, "group003", "goe002", 2, 900L));

        pool.shutdown();
        pool.awaitTermination(600, TimeUnit.SECONDS);
    }
}