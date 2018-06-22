package com.bonc.kafka110.tryandtry;

import com.bonc.kafka110.producer.IdempotentProducerThread;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * created by G.Goe on 2018/6/5
 */
public class IdempotentProducerThreadDriver {

    public static void main(String[] args) throws InterruptedException {

        long start = System.currentTimeMillis();
        String bootstrap = "172.16.40.116:29092,172.16.40.116:29093,172.16.40.116:29094,172.16.40.116:29095";
        String topic = "zhzh";

        ExecutorService pool = Executors.newCachedThreadPool();
        pool.submit(new IdempotentProducerThread(bootstrap, "client_ip_111", topic, 0, 1000000));
        // pool.submit(new IdempotentProducerThread(bootstrap, "client002", topic, 1, 300000));
        // pool.submit(new IdempotentProducerThread(bootstrap, "client003", topic, 2, 300000));

        pool.shutdown();
        pool.awaitTermination(3000, TimeUnit.SECONDS);
        long end = System.currentTimeMillis();
        System.out.println("生产耗时：" + (end - start));
    }
}
