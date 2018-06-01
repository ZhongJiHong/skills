package producer;

import com.bonc.kafka110.producer.FileProducerThread;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * created by G.Goe on 2018/6/1
 */
public class FileProducerThreadTest {

    private String bootstrap = "172.16.40.116:19093";
    private String[] files = new String[]{"D:\\admin\\item_1.txt", "D:\\admin\\item_2.txt", "D:\\admin\\item_3.txt"};

    @Test
    public void run() throws InterruptedException {

        ExecutorService pool = Executors.newFixedThreadPool(files.length);

        for (int i = 0; i < files.length; i++) {
            // 此处3 -- 表示topic的分区数
            pool.submit(new FileProducerThread(bootstrap, "client00" + i, "goe001", i % 3, files[i]));
        }

        pool.shutdown();
        pool.awaitTermination(600, TimeUnit.SECONDS);
    }
}