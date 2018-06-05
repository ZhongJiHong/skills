package com.bonc.kafka110.tryandtry;

import com.bonc.kafka110.consumer.TransactionalConsumerThread;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * created by G.Goe on 2018/6/5
 */
public class TransactionalConsumerThreadDriver {

    public static void main(String[] args) {

        String bootstrap = "172.16.40.116:19093";

        ExecutorService pool = Executors.newCachedThreadPool();
        pool.submit(new TransactionalConsumerThread(bootstrap, "tx_client001", "goe001", 0));
        pool.submit(new TransactionalConsumerThread(bootstrap, "tx_client002", "goe001", 1));
        pool.submit(new TransactionalConsumerThread(bootstrap, "tx_client003", "goe001", 2));
    }
}
