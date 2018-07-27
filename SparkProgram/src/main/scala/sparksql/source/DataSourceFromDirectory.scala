package sparksql.source

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * created by G.Goe on 2018/7/27
  */
object DataSourceFromDirectory {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("smart").setLevel(Level.WARN)

    val conf = new SparkConf()
      .setAppName("directory source")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))

    val lines = ssc.textFileStream("hdfs://bonchost:9000/zhong/sparksql/directory")
    val words = lines.flatMap(_.split("\\s+"))
    val wordCounts = words.map((_, 1)).reduceByKey(_ + _)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
