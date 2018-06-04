package com.bonc.kafka110.tryandtry;

import com.bonc.kafka110.producer.RecordsProducerThread;
import com.bonc.kafka110.utils.RecordsGenerator;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * created by G.Goe on 2018/6/4
 */
public class FileProducerDriver {

    public static void main(String[] args) throws InterruptedException {

        String bootstrap = "172.16.40.116:19093";
        String[] files = new String[]{"D:\\admin\\item_1.txt", "D:\\admin\\item_2.txt", "D:\\admin\\item_3.txt"};

        ExecutorService pool = Executors.newFixedThreadPool(files.length);

        for (int i = 0; i < files.length; i++) {
            // 此处3 -- 表示topic的分区数
            pool.submit(new RecordsProducerThread(bootstrap, "client00" + i, "goe001", i % 3,
                    RecordsGenerator.recordFromFile_1(files[i])));
        }

        pool.shutdown();
        pool.awaitTermination(6000, TimeUnit.SECONDS);
    }
}
