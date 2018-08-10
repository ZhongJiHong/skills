package sparkstreaming.sink

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * created by G.Goe on 2018/7/27
  */
object DataSinkToFile {
  def main(args: Array[String]): Unit = {
    // print() -- 控制台输出 测试时使用较多
    // saveAsTextFiles(prefix,[suffix]) -- text files
    // saveAsObjectFiles(prefix,[suffix]) -- sequenceFiles
    // saveAsHadoopFiles(prefix,[suffix]) -- Hadoop files
    // foreachRDD(func) --

    Logger.getLogger("smart").setLevel(Level.WARN)
    val spark = SparkSession
      .builder()
      .appName("hdfs sink")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(5))
    val lines = ssc.textFileStream("hdfs://bonchost:9000/zhong/sStream")
    val words = lines.flatMap(_.split("\\s+"))
    val wordCounts = words.map((_, 1)).reduceByKey(_ + _)
    wordCounts.print()

    // window测试环境下，数据存储到 D:\zhong\ouput\wordcounts-[timestamp].smart
    // Linux生产环境下，数据存储到 hdfs://bonchost:9000/zhong/output/wordcounts-[timestamp].smart
    wordCounts.saveAsTextFiles("/zhong/output/wordcounts", "smart")

    ssc.start()
    ssc.awaitTermination()
  }
}
