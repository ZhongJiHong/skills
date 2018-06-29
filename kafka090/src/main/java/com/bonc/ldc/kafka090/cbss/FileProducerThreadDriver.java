package com.bonc.ldc.kafka090.cbss;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * created by G.Goe on 2018/6/4
 */
public class FileProducerThreadDriver {

    public static void main(String[] args) throws InterruptedException {

        String bootstrap = "172.16.40.115:9093,172.16.40.116:9093";

        String filePath_1 = "D:\\tf_f_user_item.txt";
        // String filePath_2 = "D:\\tf_b_trade_develop.txt";
        // String filePath_3 = "D:\\tf_b_trade_item.txt";
        String filePath_4 = "D:\\temp_TF_B_TRADE_DEVELOP";  // RecordsGenerator.recordFromFile_2
        String filePath_5 = "D:\\temp_TF_B_TRADE_ITEM";     // RecordsGenerator.recordFromFile_2
        String filePath_6 = "D:\\cbss\\temp_TF_B_TRADE_ITEM";
        String filePath_7 = "D:\\cbss\\temp_TF_B_TRADE_ITEM.ITEM";

        ExecutorService pool = Executors.newCachedThreadPool();
        pool.submit(new FileProducerThread(bootstrap, "cbss_client007", "cbss002", 0,
                RecordsGenerator.recordFromFile_1(filePath_1)));

        pool.shutdown();
        pool.awaitTermination(6000, TimeUnit.SECONDS);


        // D:\tf_f_user_item.txt 文件，该文件的解析格式是以'\r'分隔符
        /*String filePath_1 = "D:\\tf_f_user_item.txt";
        String[] records = RecordsGenerator.recordFromFile_1(filePath_1);
        System.out.println(records.length);

        ExecutorService pool = Executors.newCachedThreadPool();
        pool.submit(new FileProducerThread(bootstrap, "cbss_client001", "cbss001", 2,
                RecordsGenerator.recordFromFile_1(filePath_1)));

        pool.shutdown();
        pool.awaitTermination(6000, TimeUnit.SECONDS);*/

        // D:\tf_b_trade_develop.txt,D:\tf_b_trade_item.txt 文件，文件的解析格式是以'\n'为分隔符
        /*String filePath_2 = "D:\\tf_b_trade_develop.txt";

        ExecutorService pool = Executors.newCachedThreadPool();
        pool.submit(new FileProducerThread(bootstrap, "cbss_client002", "cbss001", 2,
                RecordsGenerator.recordFromFile_1(filePath_2)));

        pool.shutdown();
        pool.awaitTermination(6000, TimeUnit.SECONDS);*/

        /*String filePath_3 = "D:\\tf_b_trade_item.txt";
        ExecutorService pool = Executors.newCachedThreadPool();
        pool.submit(new FileProducerThread(bootstrap, "cbss_client003", "cbss001", 2,
                RecordsGenerator.recordFromFile_2(filePath_3)));

        pool.shutdown();
        pool.awaitTermination(6000, TimeUnit.SECONDS);*/
    }
}
