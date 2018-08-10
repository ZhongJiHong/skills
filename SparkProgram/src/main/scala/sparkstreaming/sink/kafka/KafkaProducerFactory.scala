package sparkstreaming.sink.kafka

import scala.collection.mutable
import org.apache.kafka.clients.producer.KafkaProducer
import org.slf4j.LoggerFactory

/**
  * created by G.Goe on 2018/8/1
  */
object KafkaProducerFactory {

  import scala.collection.JavaConverters._

  private val logger = LoggerFactory.getLogger(this.getClass)

  private val producers = mutable.Map[Map[String, Object], KafkaProducer[String, String]]()

  def getOrCreateProducer(config: Map[String, Object]): KafkaProducer[String, String] = {
    val defaultConfig = Map[String, Object](
      "key.serializer" -> "org.apache.kafka.common.serialization.ByteArraySerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.ByteArraySerializer"
    )

    val finalConfig = defaultConfig ++ config

    producers.getOrElseUpdate(finalConfig, {
      logger.info(s"Create Kafka producer , config: $finalConfig")
      val producer = new KafkaProducer[String, String](finalConfig.asJava)
      sys.addShutdownHook {
        logger.info(s"Close Kafka producer, config: $finalConfig")
        producer.close()
      }
      producer
    })
  }
}

// 自定义一个可序列化的KafkaProducer类