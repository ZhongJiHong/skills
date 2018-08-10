package structuredstreaming.source

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * created by G.Goe on 2018/7/30
  */
// 验证结果,控制台去除日志信息,响应的速度还是可以的
object DataSourceFromSocket {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("smart").setLevel(Level.WARN)
    val spark = SparkSession
      .builder()
      .appName("socket source")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._
    val lines = spark.readStream
      .format("socket")
      .option("host", "bonchost")
      .option("port", 9996)
      .load()
    val words = lines.as[String].flatMap(_.split("\\s+"))
    val wordCounts = words.groupBy("value").count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()
    query.awaitTermination()
  }
}
