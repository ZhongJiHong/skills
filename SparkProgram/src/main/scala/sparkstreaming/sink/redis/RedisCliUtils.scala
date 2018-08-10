package sparkstreaming.sink.redis

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{HostAndPort, JedisCluster, JedisPool}

/**
  * created by G.Goe on 2018/8/2
  */
object RedisCliUtils {
  /*val host = "172.16.40.115"
  val port = 7000
  lazy val pool = new JedisPool(new GenericObjectPoolConfig, host, port, TimeOut)*/

  val TimeOut = 30000

  import scala.collection.JavaConversions._

  val nodes = Set(
    new HostAndPort("kafka02", 7000),
    new HostAndPort("kafka02", 7001),
    new HostAndPort("kafka02", 7002),
    new HostAndPort("kafka02", 7003),
    new HostAndPort("kafka02", 7004),
    new HostAndPort("kafka02", 7005),
    new HostAndPort("kafka02", 7006),
    new HostAndPort("kafka02", 7007)

  )
  lazy val jedisCluster = new JedisCluster(nodes, TimeOut)

  lazy val hook = new Thread {
    override def run(): Unit = {
      println("Execute hook thread:" + this)
      jedisCluster.close()
    }
  }
  // 钩子函数 -- 在JVM中注册一个关闭的钩子(hook),当JVM关闭的时候，会执行系统中已经设置的所有通过方法addShutdownHook()注册的钩子，
  // 当系统执行完这些钩子后，JVM才会关闭，所以这些钩子可以在JVM关闭的时候进行内存清理、对象销毁等操作。
  sys.addShutdownHook(hook.run())
}
