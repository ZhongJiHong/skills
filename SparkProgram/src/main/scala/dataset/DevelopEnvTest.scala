package dataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * created by G.Goe on 2018/7/26
  */
/*
  测试本工程环境搭建是否满足正确合理
 */
object DevelopEnvTest {

  def main(args: Array[String]): Unit = {

    // 初始化上下文对象
    val spark = SparkSession
      .builder()
      .appName("Develop Env Test")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._
    val sc = spark.sparkContext

    // --------------------------- SparkSQL 开发环境测试 ---------------------------------------
    // val rdd = sc.textFile("hdfs://bonchost:9000/zhong/person")
    // val df = rdd.map(_.split("\\s+")).map(p => (p(0), p(1))).toDF("name", "age")

    // UnTyped API
    // df.select($"name", $"age").where($"name" === "zhong").show()

    // Typed API
    // TODO

    // Sql
    // df.createOrReplaceTempView("consumer")
    // spark.sql("select * from consumer where name = 'zhong'").show()

    // --------------------------- SparkStreaming 开发环境测试 -----------------------------------
    /*val ssc = new StreamingContext(sc, Seconds(5))

    val lines = ssc.socketTextStream("bonchost", 9999, StorageLevel.MEMORY_ONLY)
    val wordCounts = lines.flatMap(_.split("\\s+")).countByValue().print()

    ssc.start()
    ssc.awaitTermination()*/

    // --------------------------- StructsStreaming 开发环境测试 ---------------------------------
    // TODO
  }
}
