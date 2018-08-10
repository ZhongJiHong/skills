package sparkstreaming.sink.redis

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

/**
  * created by G.Goe on 2018/8/2
  */
object DataSinkToRedis {
  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("redis sink")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(2))
    val lines = ssc.socketTextStream("bonchost", 9996, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split("\\s+"))
    val wordCounts = words.map((_, 1)).reduceByKey(_ + _)

    wordCounts.foreachRDD {
      rdd => {
        rdd.foreachPartition {
          records => {
            records.foreach {
              record => {
                val jedisCluster = RedisCliUtils.jedisCluster
                jedisCluster.set(record._1, record._2.toString)
              }
            }
          }
        }
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
