package sparkstreaming.sink.hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * created by G.Goe on 2018/8/2
  */
object DataSinkToHbase {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("hbase sink")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(5))

    val lines = ssc.textFileStream("hdfs://bonchost:9000/zhong/sStream")
    val words = lines.flatMap(_.split("\\s+"))
    val wordCounts = words.map((_, 1)).reduceByKey(_ + _)

    val tableNmae = "zjh_test"
    wordCounts.foreachRDD {
      rdd => {
        rdd.foreachPartition {
          records => {
            // TODO -- Hbase未测试
            val conf = HBaseConfiguration.create()
            val customTable = new HTable(conf, TableName.valueOf(tableNmae))
            customTable.setAutoFlush(false, false)
            customTable.setWriteBufferSize(3 * 1024 * 1024)
            records.foreach {
              record => {
                val key = new Put(Bytes.toBytes(record._1))
                key.add(Bytes.toBytes("userinfo"), "as".getBytes(), Bytes.toBytes(record._2))
                customTable.put(key)
              }
            }
            customTable.flushCommits()
          }
        }
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
