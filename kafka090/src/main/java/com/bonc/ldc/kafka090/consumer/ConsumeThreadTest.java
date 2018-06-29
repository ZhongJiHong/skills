package com.bonc.ldc.kafka090.consumer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * created by G.Goe on 2018/5/4
 */
public class ConsumeThreadTest {

    public static void main(String[] args) throws InterruptedException {

        ExecutorService pool = Executors.newCachedThreadPool();
        ConsumerThread consumerThread = new ConsumerThread("172.16.40.115:9093,172.16.40.116:9093", "test_encoding",
                "cbss002", 1, 0L);
        pool.submit(consumerThread);
        /*pool.submit(new ConsumerThread("172.16.40.114:9093,172.16.40.115:9093,172.16.40.116:9093", "test",
                "test", 1, 0L));
        pool.submit(new ConsumerThread("172.16.40.114:9093,172.16.40.115:9093,172.16.40.116:9093", "test",
                "test", 2, 0L));*/

        Thread.sleep(6000);
        pool.submit(new Thread() {
            @Override
            public void run() {
                consumerThread.shutdown();
            }
        });

        pool.shutdown();
        pool.awaitTermination(3000, TimeUnit.SECONDS);
    }
}
