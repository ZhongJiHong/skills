package sparksql.source

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * created by G.Goe on 2018/7/27
  */
object DataSourceFromKafka {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("smart").setLevel(Level.WARN)
    val spark = SparkSession
      .builder()
      .appName("kafka source")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(10))

    val topics = Array("test01", "test02")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "node01:9092,node02:9092,node03:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // 高级数据源 -- 需要外部的依赖
    // 更多信息查看 -- http://spark.apache.org/docs/2.3.0/streaming-kafka-0-10-integration.html
    val records = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe(topics, kafkaParams))
    records.map(_.value()).print()

    ssc.start() // TODO
    ssc.awaitTermination()
  }
}
