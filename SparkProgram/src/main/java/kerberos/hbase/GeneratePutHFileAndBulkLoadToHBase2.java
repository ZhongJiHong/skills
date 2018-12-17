package kerberos.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * 杩愯eg :
 * HADOOP_CLASSPATH=/opt/beh/core/hbase/lib/*  hadoop jar Test.jar BulkLoad  /test.txt /dltest dltest ',' ff q1,q2,q3 q1 127.0.0.1 2181
 */


public class GeneratePutHFileAndBulkLoadToHBase2 {

    private static final Logger LOGGER = LoggerFactory.getLogger(
            GeneratePutHFileAndBulkLoadToHBase2.class);

    public static class ConvertImportToHFileMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>

    {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String separator = context.getConfiguration().get("separator");
            String f = context.getConfiguration().get("family");
            String[] fieldsName = context.getConfiguration().get("fieldsName").split(",");
            String[] rowkeyCols = context.getConfiguration().get("rowkeyCol").split(",");
            String str = value.toString();
            String[] strArray = str.split(separator);

            List<String> rowkeyIndex = Arrays.asList(fieldsName);
            StringBuilder word = new StringBuilder();
            for (int i = 0; i < rowkeyCols.length; i++) {
                word.append(strArray[rowkeyIndex.indexOf(rowkeyCols[i])]).append("_");
            }
            word = word.deleteCharAt(word.length() - 1);

            byte[] rowKey = Bytes.toBytes(word.toString());
            ImmutableBytesWritable rowKeyWritable = new ImmutableBytesWritable(rowKey);
            byte[] family = Bytes.toBytes(f);

            for (int i = 0; i < fieldsName.length; i++) {
                byte[] qualifier = Bytes.toBytes(fieldsName[i]);
                byte[] hbaseValue = Bytes.toBytes(strArray[i]);
                Put put = new Put(rowKey);
                put.addColumn(family, qualifier, hbaseValue);
                context.write(rowKeyWritable, put);
            }
        }
    }


    public static void main(String[] args)
            throws Exception {
        Configuration hadoopConfiguration = new Configuration();
        String[] dfsArgs = new GenericOptionsParser(hadoopConfiguration, args).getRemainingArgs();

        if (dfsArgs.length < 9) {
            LOGGER.warn("Usage: hbase_import <in> <out>  <tableName> <field_separator> <family> <qualify> <rowkey_column> <zookeeperIP> <zookeeperPort>");
            System.exit(2);
        }

        FileSystem fs = FileSystem.get(hadoopConfiguration);
        if (fs.exists(new Path(dfsArgs[1]))) {
            fs.delete(new Path(dfsArgs[1]), true);
        }

        hadoopConfiguration.set("separator", dfsArgs[3]);
        hadoopConfiguration.set("family", dfsArgs[4]);
        hadoopConfiguration.set("fieldsName", dfsArgs[5]);
        hadoopConfiguration.set("rowkeyCol", dfsArgs[6]);

        Configuration hbaseConfiguration;
        int convertWordCountJobOutputToHFileJobResult;
        LoadIncrementalHFiles loader;


        try (Job convertImportJobOutputToHFileJob = Job.getInstance(hadoopConfiguration, "import_bulkload")) {

            // 第一步,生成HFile文件
            convertImportJobOutputToHFileJob.setJarByClass(GeneratePutHFileAndBulkLoadToHBase2.class);
            convertImportJobOutputToHFileJob.setMapperClass(ConvertImportToHFileMapper.class);

            convertImportJobOutputToHFileJob.setMapOutputKeyClass(ImmutableBytesWritable.class);
            convertImportJobOutputToHFileJob.setMapOutputValueClass(Put.class);

            FileInputFormat.addInputPath(convertImportJobOutputToHFileJob, new Path(dfsArgs[0]));
            FileOutputFormat.setOutputPath(convertImportJobOutputToHFileJob, new Path(dfsArgs[1]));

            // 第二步,使用BulkLoader导入Hbase
            hbaseConfiguration = HBaseConfiguration.create();
            hbaseConfiguration.set("hbase.zookeeper.quorum", dfsArgs[7]);
            hbaseConfiguration.set("hbase.zookeeper.property.clientPort", dfsArgs[8]);
            String tableName = dfsArgs[2];


            // 重构 TODO -- 还需要完善整个代码的顺序
            try (Connection connection = ConnectionFactory.createConnection(hbaseConfiguration)) {
                Table table = connection.getTable(TableName.valueOf(tableName));
                RegionLocator regionLocator = new HRegionLocator(TableName.valueOf(dfsArgs[2]), (ClusterConnection) connection);
                HFileOutputFormat2.configureIncrementalLoad(convertImportJobOutputToHFileJob, table, regionLocator);
                convertWordCountJobOutputToHFileJobResult = convertImportJobOutputToHFileJob.waitForCompletion(true) ? 0 : 1;
                loader = new LoadIncrementalHFiles(hbaseConfiguration);
                loader.doBulkLoad(new Path(dfsArgs[1]), connection.getAdmin(), table, regionLocator);
            }

        }
        // 返回执行的结果,0表示成功,1表示失败
        System.exit(convertWordCountJobOutputToHFileJobResult);
    }

}

