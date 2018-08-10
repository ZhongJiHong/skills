package structuredstreaming.sink.file

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
  * created by G.Goe on 2018/8/2
  */
object DataSinkToParquetFile {
  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("parquet sink")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._

    val df = spark.readStream
      .format("socket")
      .option("host", "bonchost")
      .option("port", 9996)
      .load() // DF

    val lines = df.as[String] // DF -> DS
    val words = lines.flatMap(_.split("\\s+"))
    val wordCounts = words.groupBy("value").count()

    val query = wordCounts.writeStream
      .outputMode("append")
      .format("parquet")
      .option("checkpointLocation", "D:\\User\\Test\\checkpoint")
      .option("path", "D:\\User\\Test\\output")
      .start()
    query.awaitTermination()
  }
}
