package sparksql.source

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * created by G.Goe on 2018/7/27
  */
object DataSourceFromSocket {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.saprk").setLevel(Level.WARN)

    /*val spark = SparkSession
      .builder()
      .appName("socket source")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext*/
    val conf = new SparkConf()
      .setAppName("socket source")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(5))
    val lines = ssc.socketTextStream("bonchost", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split("\\s+"))
    val wordCounts = words.map((_, 1)).reduceByKey(_ + _).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
