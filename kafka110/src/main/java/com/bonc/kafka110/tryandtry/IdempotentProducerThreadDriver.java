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
        String bootstrap = "172.16.40.116:19093";
        String topic = "goe001";

        ExecutorService pool = Executors.newCachedThreadPool();
        pool.submit(new IdempotentProducerThread(bootstrap, "client001", topic, 0, 20000));
        pool.submit(new IdempotentProducerThread(bootstrap, "client002", topic, 1, 20000));
        pool.submit(new IdempotentProducerThread(bootstrap, "client003", topic, 2, 20000));

        pool.shutdown();
        pool.awaitTermination(3000, TimeUnit.SECONDS);
        long end = System.currentTimeMillis();
        System.err.println("生产耗时：" + (end - start));
    }
}
