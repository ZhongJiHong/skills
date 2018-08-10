package sparkstreaming.sink.kafka

import java.util.concurrent.Future

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

/**
  * created by G.Goe on 2018/8/1
  */
// 自定义一个可序列化的KafkaProducer类 -- 参数是一个函数字面量??? -- 装饰者模式
class CustomKafkaProducer[K, V](createProducer: () => KafkaProducer[K, V]) extends Serializable {
  lazy val producer = createProducer()

  def send(topic: String, key: K, value: V): Future[RecordMetadata] =
    producer.send(new ProducerRecord[K, V](topic, key, value))

  def send(topic: String, value: V): Future[RecordMetadata] =
    producer.send(new ProducerRecord[K, V](topic, value))
}

object CustomKafkaProducer {

  import scala.collection.JavaConversions._

  def apply[K, V](config: Map[String, Object]): CustomKafkaProducer[K, V] = {
    val createProducerFunc = () => {
      val producer = new KafkaProducer[K, V](config)
      sys.addShutdownHook {
        producer.close()
      }
      producer
    }
    new CustomKafkaProducer(createProducerFunc)
  }

  def apply[K, V](config: java.util.Properties): CustomKafkaProducer[K, V] = apply(config.toMap)
}
