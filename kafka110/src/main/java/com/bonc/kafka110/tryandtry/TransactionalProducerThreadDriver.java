package com.bonc.kafka110.tryandtry;

import com.bonc.kafka110.producer.TransactionalProducerThread;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * created by G.Goe on 2018/6/11
 */
public class TransactionalProducerThreadDriver {

    public static void main(String[] args) throws InterruptedException {

        long start = System.currentTimeMillis();
        /*String bootstrap = "172.16.40.116:29093,172.16.40.116:29094,172.16.40.116:29095,172.16.40.116:29096";
        String topic = "goe116";*/
        String bootstrap = "172.16.40.116:29092,172.16.40.116:29093,172.16.40.116:29094,172.16.40.116:29095";
        String topic = "zhzh";

        ExecutorService pool = Executors.newCachedThreadPool();
        // 随机产生事务id
        // UUID uuid = UUID.randomUUID();
        // 0211  0213  0215
        pool.submit(new TransactionalProducerThread(bootstrap, "tx_0212", "cleint_tx_000", topic, 0, 1000000));
        // pool.submit(new TransactionalProducerThread(bootrap, "transactional02", topic, 1, 300000));
        // pool.submit(new TransactionalProducerThread(bootrap, "transactional03", topic, 2, 300000));

        pool.shutdown();
        pool.awaitTermination(3000, TimeUnit.SECONDS);
        long end = System.currentTimeMillis();
        System.out.println("生产耗时：" + (end - start));
    }
}
