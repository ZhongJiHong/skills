package structuredstreaming.source

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * created by G.Goe on 2018/7/30
  */
object DataSourceFromKafka {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("smart").setLevel(Level.WARN)

    val spark = SparkSession
      .builder()
      .appName("kafka source")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    val records = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka01:9092,kafka02:9092,kafka03:9092")
      // 关于kafka更多的细节，查看网址 http://spark.apache.org/docs/2.3.0/structured-streaming-kafka-integration.html
      .option("subscribe", "zjh_spark01,zjh_spark02") // 订阅多个Topic
      .load()

    val tuples = records.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]

    val words = tuples.map(_._2).flatMap(_.split("\\s+"))
    // val wordCounts = words.groupBy("value").count()

    val query = words.writeStream
      .outputMode("append")
      .format("console")
      .start()
    query.awaitTermination() // TODO--验证,无法输出结果

  }
}
