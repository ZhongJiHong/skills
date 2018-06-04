package com.bonc.kafka110.tryandtry;

import com.bonc.kafka110.utils.RecordsGenerator;

import java.io.UnsupportedEncodingException;

/**
 * created by G.Goe on 2018/6/1
 */
public class JustTry {

    public static void main(String[] args) throws Exception {

        // ---------------------------------测试文件的行分隔符'\r'还是'\n'--------------------------------------
        // D:\admin\\item_1.txt
        // D:\tf_f_user_item.txt
        /*String filePath_1 = "D:\\tf_f_user_item.txt";
        String[] records = RecordsGenerator.recordFromFile_1(filePath_1);*/

        // D:\admin\temp_1
        // D:\temp_TF_B_TRADE_DEVELOP
        // D:\temp_TF_B_TRADE_ITEM
        /*String filePath_2 = "D:\\temp_TF_B_TRADE_ITEM";
        String[] records = RecordsGenerator.recordFromFile_2(filePath_2);*/

        // System.out.println(records.length);
        // --------------------------------------------------------------------------------------------------


        System.out.println("key是分区号，value是写死的，就像这样！".getBytes("UTF-8").length);
    }
}
