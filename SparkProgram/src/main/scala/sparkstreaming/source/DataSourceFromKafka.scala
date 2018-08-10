package sparkstreaming.source

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * created by G.Goe on 2018/7/27
  *
  * @author G.Goe
  */
// 验证结果,响应速度满足基本测试需求
object DataSourceFromKafka {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("smart").setLevel(Level.WARN)
    val spark = SparkSession
      .builder()
      .appName("kafka source")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(5))

    val topics = Array("zjh_spark01", "zjh_spark02")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "kafka01:9092,kafka02:9092,kafka03:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // 高级数据源 -- 需要外部的依赖
    // 更多信息查看 -- http://spark.apache.org/docs/2.3.0/streaming-kafka-0-10-integration.html
    val records = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))
    val results = records.map(_.value()).flatMap(_.split("\\s+")).map((_, 1)).reduceByKey(_ + _).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
