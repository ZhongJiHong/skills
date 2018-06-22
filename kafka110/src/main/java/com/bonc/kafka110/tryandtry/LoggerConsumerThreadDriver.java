package com.bonc.kafka110.tryandtry;

import com.bonc.kafka110.consumer.LoggerConsumerThread;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * created by G.Goe on 2018/6/4
 */
public class LoggerConsumerThreadDriver {

    public static void main(String[] args) throws InterruptedException {

        long start = System.currentTimeMillis();
        String bootstrap = "172.16.40.116:19093";
        String topic = "goe011";

        ExecutorService pool = Executors.newCachedThreadPool();
        pool.submit(new LoggerConsumerThread(bootstrap, "group01101", topic, 0, 0L));
        pool.submit(new LoggerConsumerThread(bootstrap, "group01102", topic, 1, 0L));
        pool.submit(new LoggerConsumerThread(bootstrap, "group01103", topic, 2, 0L));

        pool.shutdown();
        pool.awaitTermination(3000, TimeUnit.SECONDS);
        long end = System.currentTimeMillis();
        System.err.println("消费耗时：" + (end - start));
    }
}
