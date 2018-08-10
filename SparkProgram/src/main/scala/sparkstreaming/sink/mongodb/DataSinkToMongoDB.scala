package sparkstreaming.sink.mongodb

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * created by G.Goe on 2018/8/2
  */
object DataSinkToMongoDB {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("MongoDB sink")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(5))

    val topicsc = Set("zjh_spark01", "zjh_spark02")
    val kafkaParams = Map[String, Object](
      "kafka.bootstrap.servers" -> "kafka01:9092,kafka02:9092,kafka03:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "smart_G2",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val records = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsc, kafkaParams))
    val wordCounts = records.map(_.value()).flatMap(_.split("\\s+")).map((_, 1)).reduceByKey(_ + _)

    wordCounts.foreachRDD {
      rdd => {
        // TODO -- MongoDB未实现
      }
    }
  }
}
