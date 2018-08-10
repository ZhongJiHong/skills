package structuredstreaming.source

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * created by G.Goe on 2018/7/30
  */
// 虽说测试通过，但是响应的速度明显太差
object DataSourceFromDirectory {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("smart").setLevel(Level.WARN)
    val spark = SparkSession
      .builder()
      .appName("directory source")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._
    val lines = spark.readStream
      .text("hdfs://bonchost:9000/zhong/sStream")

    val words = lines.as[String].flatMap(_.split("\\s+"))
    val wordCounts = words.groupBy("value").count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
