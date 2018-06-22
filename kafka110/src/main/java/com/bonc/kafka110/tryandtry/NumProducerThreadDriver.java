package com.bonc.kafka110.tryandtry;

import com.bonc.kafka110.producer.NumProducerThread;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * created by G.Goe on 2018/6/4
 */
public class NumProducerThreadDriver {

    public static void main(String[] args) throws InterruptedException {

        long start = System.currentTimeMillis();
        String bootstrap = "172.16.40.116:29092,172.16.40.116:29093,172.16.40.116:29094,172.16.40.116:29095";
        String topic = "zhzh";

        ExecutorService pool = Executors.newCachedThreadPool();
        pool.submit(new NumProducerThread(bootstrap, "client001", topic, 3, 500000));
        // pool.submit(new NumProducerThread(bootstrap, "client002", topic, 1, 500000));
        // pool.submit(new NumProducerThread(bootstrap, "client003", topic, 2, 500000));

        pool.shutdown();
        pool.awaitTermination(3000, TimeUnit.SECONDS);
        long end = System.currentTimeMillis();
        System.out.println("生产耗时：" + (end - start));
    }
}
