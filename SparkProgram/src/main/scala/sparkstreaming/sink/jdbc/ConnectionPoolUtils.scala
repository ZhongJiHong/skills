package sparkstreaming.sink.jdbc

import java.sql.Connection

import com.jolbox.bonecp.{BoneCP, BoneCPConfig}
import org.slf4j.LoggerFactory

/**
  * created by G.Goe on 2018/8/1
  */
object ConnectionPoolUtils {
  val logger = LoggerFactory.getLogger(this.getClass)
  private val connectionPool: Option[BoneCP] = {
    try {
      Class.forName("com.mysql.jdbc.Driver")
      val config = new BoneCPConfig()
      config.setJdbcUrl("jdbc:mysql://bonchost:3306/test")
      config.setUsername("bonc")
      config.setPassword("bonc")
      config.setLazyInit(true)

      config.setMinConnectionsPerPartition(3)
      config.setMaxConnectionsPerPartition(5)
      config.setPartitionCount(5)
      config.setCloseConnectionWatch(true)
      config.setLogStatementsEnabled(false)
      Some(new BoneCP(config))
    } catch {
      case e: Exception => {
        logger.warn("ConnectionPool created Failed.+\n" + e.printStackTrace())
        None
      }
    }
  }

  def getConnection: Option[Connection] = {
    connectionPool match {
      case Some(pool) => Some(pool.getConnection)
      case None => None
    }
  }

  def closeConnection(connection: Connection): Unit = {
    if (!connection.isClosed) {
      connection.close()
    }
  }
}
