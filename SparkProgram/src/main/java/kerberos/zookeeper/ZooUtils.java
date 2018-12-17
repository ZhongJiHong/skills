package kerberos.zookeeper;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * @Author G.Goe
 * @Date 2018/9/26
 * @Request JDK1.8及以上版本
 * @Resource
 */
public class ZooUtils {

    private ZooUtils() {
    }

    private static Logger logger = LoggerFactory.getLogger(ZooUtils.class);

    // 需要处理
    private static Watcher watcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeDataChanged) {
                logger.info("NodeData changed.");
            } else if (event.getType() == Event.EventType.NodeChildrenChanged) {
                logger.info("NodeChildren Changed!");
            } else if (event.getType() == Event.EventType.NodeCreated) {
                logger.info("Node Created");
            } else if (event.getType() == Event.EventType.NodeDeleted) {
                logger.info("Node Deleted");
            }
        }
    };

    static {
        // 设置环境变量
        System.setProperty("java.security.krb5.conf", "D:\\admin\\data\\krb5.conf");
        System.setProperty("sun.security.krb5.debug", "true");

        System.setProperty("java.security.auth.login.config", "D:\\admin\\data\\jaas.conf");
    }


    /**
     * 获取客户端连接
     *
     * @param bootstrap
     * @param sessionTimeout
     * @return
     * @throws IOException
     */
    public static ZooKeeper getClient(String bootstrap, int sessionTimeout) throws IOException {
        return new ZooKeeper(bootstrap, sessionTimeout, watcher);
    }

    /**
     * 创建持久化节点
     *
     * @param zk
     * @param path
     * @param data
     * @param acls
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static void createNode(ZooKeeper zk, String path, String data, List<ACL> acls) throws KeeperException, InterruptedException {
        zk.create(path, Bytes.toBytes(data), acls, CreateMode.PERSISTENT);
    }

    /**
     * 获取自定节点的数据
     *
     * @param zk
     * @param path
     * @param stat
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static byte[] getData(ZooKeeper zk, String path, Stat stat) throws KeeperException, InterruptedException {
        return zk.getData(path, watcher, stat);
    }


    /**
     * 更新路径节点的数据
     *
     * @param zk
     * @param path
     * @param data
     * @param version
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static void updateData(ZooKeeper zk, String path, String data, int version) throws KeeperException, InterruptedException {
        zk.setData(path, Bytes.toBytes(data), version);
    }

    /**
     * 获取指定路径节点的ACL
     *
     * @param zk
     * @param path
     * @param stat
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static List<ACL> getACL(ZooKeeper zk, String path, Stat stat) throws KeeperException, InterruptedException {

        return zk.getACL(path, stat);
    }

    /**
     * 获取子节点
     *
     * @param zk
     * @param path
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static List<String> getChildren(ZooKeeper zk, String path) throws KeeperException, InterruptedException {
        return zk.getChildren(path, watcher);
    }

    /**
     * 删除节点
     *
     * @param zk
     * @param path
     * @param version
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static void deleteNode(ZooKeeper zk, String path, int version) throws KeeperException, InterruptedException {
        zk.delete(path, version);
    }
}
