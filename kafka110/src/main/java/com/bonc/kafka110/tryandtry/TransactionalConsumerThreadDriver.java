package com.bonc.kafka110.tryandtry;

import com.bonc.kafka110.consumer.TransactionalConsumerThread;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * created by G.Goe on 2018/6/5
 */
public class TransactionalConsumerThreadDriver {

    public static void main(String[] args) {

        /*String bootstrap = "172.16.40.116:29093,172.16.40.116:29094,172.16.40.116:29095,172.16.40.116:29096";
        String topic = "goe116";*/

        String bootstrap = "172.16.40.116:29092,172.16.40.116:29093,172.16.40.116:29094,172.16.40.116:29095";
        String topic = "__transaction_state";

        ExecutorService pool = Executors.newCachedThreadPool();
        // pool.submit(new TransactionalConsumerThread(bootstrap, "tx_client001", topic, 0));
        // pool.submit(new TransactionalConsumerThread(bootstrap, "tx_client002", topic, 1));
        pool.submit(new TransactionalConsumerThread(bootstrap, "tx_client007", topic, 0));
    }
}
