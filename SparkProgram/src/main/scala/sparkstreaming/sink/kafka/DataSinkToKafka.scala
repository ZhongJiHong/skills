package sparkstreaming.sink.kafka

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

/**
  * created by G.Goe on 2018/8/1
  */
object DataSinkToKafka {
  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("kafka sink")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(5))

    val topics = Set("zjh_spark01")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "kafka01:9092,kafka02:9092,kafka03:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val records = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))
    val wordCounts = records.map(_.value()).flatMap(_.split("\\s+")).map((_, 1)).reduceByKey(_ + _)

    val producerConfig = Map[String, Object](
      "bootstrap.servers" -> "kafka01:9092,kafka02:9092,kafka03:9092",
      "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
    )
    // val kafkaProducer = KafkaProducerFactory.getOrCreateProducer(producerConfig)
    val kafkaProducer = CustomKafkaProducer[String, String](producerConfig)
    val producer = spark.sparkContext.broadcast(kafkaProducer)
    wordCounts.foreachRDD {
      rdd => {
        rdd.foreachPartition {
          partitionOfRecord => {
            partitionOfRecord.foreach {
              record => {
                // producer.value.send(new ProducerRecord[String, String]("zjh_spark02", record._1 + "===" + record._2))
                producer.value.send("zjh_spark02", (record._1 + "===" + record._2))
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
