package com.bonc.kafka110.tryandtry;

import com.bonc.kafka110.utils.RecordsGenerator;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.UUID;

/**
 * created by G.Goe on 2018/6/1
 */
public class JustTry {

    public static void main(String[] args) throws Exception {

        // ---------------------------------测试文件的行分隔符'\r'还是'\n'--------------------------------------
        // D:\admin\\item_1.txt
        // D:\tf_f_user_item.txt
        /*String filePath_1 = "D:\\tf_f_user_item.txt";
        String filePath_2 = "D:\\tf_b_trade_develop.txt";
        String filePath_3 = "D:\\tf_b_trade_item.txt";
        String filePath_4 = "D:\\temp_TF_B_TRADE_DEVELOP";  // RecordsGenerator.recordFromFile_2
        String filePath_5 = "D:\\temp_TF_B_TRADE_ITEM";  // RecordsGenerator.recordFromFile_2
        String filePath_6 = "D:\\cbss\\temp_TF_B_TRADE_ITEM";
        String filePath_7 = "D:\\cbss\\temp_TF_B_TRADE_ITEM.ITEM";

        String[] records = RecordsGenerator.recordFromFile_2(filePath_6);
        System.out.println(records.length);*/

        UUID uuid = UUID.randomUUID();
        String s = uuid.toString();
        System.out.println(s);

        // 13 10

        /*File file = new File(filePath_2);

        byte[] buffer = null;

        try (
                FileInputStream input = new FileInputStream(file)
        ) {
            buffer = new byte[(int) file.length()];

            // 将数据读取到内存中
            input.read(buffer);
        } catch (Exception e) {
            e.printStackTrace();
        }
        char a = 13;
        char b = 10;

        String data = new String(buffer);

        System.out.println(data.indexOf("MIIBmQYJKoZIhvcNAQcDoIIBijCCAYYCAQAxggESMIIBDgIBADB3MHAxCzAJBgNV"));

        byte[] buf = new byte[500];
        System.arraycopy(buffer, 94062, buf, 0, buf.length);
        System.out.println(Arrays.toString(buf));
        System.out.println(new String(buf));*/

        // D:\admin\temp_1
        // D:\temp_TF_B_TRADE_DEVELOP
        // D:\temp_TF_B_TRADE_ITEM
        /*String filePath_2 = "D:\\temp_TF_B_TRADE_ITEM";
        String[] records = RecordsGenerator.recordFromFile_2(filePath_2);*/

        // --------------------------------------------------------------------------------------------------

        /*Schema schema = new Schema(new Field("name", Schema.STRING), new Field("age", Schema.INT8));
        Struct struct = new Struct(schema).set("name", "G.Goe").set("age", 24);
        System.out.println(struct);

        ConfigDef configDef = new ConfigDef();*/

    }
}
