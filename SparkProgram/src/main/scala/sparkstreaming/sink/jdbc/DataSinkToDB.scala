package sparkstreaming.sink.jdbc

import java.sql.Connection

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

/**
  * created by G.Goe on 2018/8/1
  */
object DataSinkToDB {
  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("DB sink")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(5))

    val lines = ssc.textFileStream("hdfs://bonchost:9000/zhong/sStream")
    val words = lines.flatMap(_.split("\\s+"))
    val wordCounts = words.map((_, 1)).reduceByKey(_ + _)
    // wordCounts.print()

    // 特别注意此处对于连接DB的处理,详细细节查看 http://spark.apachecn.org/docs/cn/2.2.0/streaming-programming-guide.html#dstreams-上的输出操作
    val sql = "insert into output(word,count) values (?,?)"
    wordCounts.foreachRDD {
      rdd => {
        val count = rdd.count()
        if (count > 0) {
          rdd.foreachPartition {
            partitionOfRecord => {
              // println(partitionOfRecord.count(p => true))
              val conn = ConnectionPoolUtils.getConnection.orNull
              if (conn != null) {
                partitionOfRecord.foreach {
                  record => {
                    insertRecord(conn, sql, record)
                  }
                }
                ConnectionPoolUtils.closeConnection(conn)
              }
            }
          }
        }
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }

  def insertRecord(conn: Connection, sql: String, record: (String, Int)) = {
    try {
      val preparedStatement = conn.prepareStatement(sql)
      preparedStatement.setString(1, record._1)
      preparedStatement.setInt(2, record._2)
      preparedStatement.executeUpdate()
      preparedStatement.close()
    } catch {
      case e: Exception => logger.error("Error message:" + e.getMessage)
    }
  }
}
