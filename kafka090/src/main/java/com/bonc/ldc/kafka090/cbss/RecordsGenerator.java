package com.bonc.ldc.kafka090.cbss;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;

/**
 * created by G.Goe on 2018/6/4
 * <p>
 * records生成器
 */
public class RecordsGenerator {

    private static Logger logger = LoggerFactory.getLogger(RecordsGenerator.class);

    // 工具类，私有化构造函数
    private RecordsGenerator() {
    }

    /**
     * 以'\r'分隔符分隔数据，产生records
     *
     * @param filePath - 数据文件本地路径
     * @return
     */
    public static String[] recordFromFile_1(String filePath) {

        File file = new File(filePath);

        byte[] buffer = null;

        try (
                FileInputStream input = new FileInputStream(file)
        ) {
            buffer = new byte[(int) file.length()];

            // 将数据读取到内存中
            input.read(buffer);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

        String data = new String(buffer);

        // 文件数据的解析规则 - 注意不同的数据文件可能有不同的解析规则，需自定义
        return data.split("\\r");
    }

    /**
     * 以'\n'分隔符分隔数据，产生records
     *
     * @param filePath - 数据文件本地路径
     * @return
     */
    public static String[] recordFromFile_2(String filePath) {

        File file = new File(filePath);

        byte[] buffer = null;

        try (
                FileInputStream input = new FileInputStream(file)
        ) {
            buffer = new byte[(int) file.length()];

            // 将数据读取到内存中
            input.read(buffer);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

        String data = new String(buffer);

        // 文件数据的解析规则 - 注意不同的数据文件可能有不同的解析规则，需自定义
        return data.split("\\n");
    }

    /**
     * 以'\r\n'分隔符分隔数据，产生records
     *
     * @param filePath - 数据文件本地路径
     * @return
     */
    public static String[] recordFromFile_3(String filePath) {

        File file = new File(filePath);

        byte[] buffer = null;

        try (
                FileInputStream input = new FileInputStream(file)
        ) {
            buffer = new byte[(int) file.length()];

            // 将数据读取到内存中
            input.read(buffer);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

        String data = new String(buffer);

        // 文件数据的解析规则 - 注意不同的数据文件可能有不同的解析规则，需自定义
        return data.split("\\r\\n");
    }
}
