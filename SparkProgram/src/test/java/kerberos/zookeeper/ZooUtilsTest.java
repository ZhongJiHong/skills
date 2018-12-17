package kerberos.zookeeper;

import kerberos.zookeeper.ZooUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @Author G.Goe
 * @Date 2018/9/27
 * @Request
 * @Resource
 */
public class ZooUtilsTest {

    private static ZooKeeper zk = null;

    @Before
    public void initEnv() throws IOException {
        zk = ZooUtils.getClient("172.16.40.33:2181,172.16.40.32:2181,172.16.40.31:2181", 3000);
//        zk = ZooUtils.getClient("172.16.40.33:2181,172.16.40.32:2181,172.16.40.31:2181", 3000);
    }

    @Test
    public void zkInfo() throws InterruptedException {
        try {
            // 输出认证信息
            // 未找到API
        } finally {
            zk.close();
        }
    }

    @Test
    public void createNode() throws KeeperException, InterruptedException {

        try {
            // List<ACL> acls = new LinkedList<>();
            // acls.add(new ACL(9, ZooDefs.Ids.AUTH_IDS));
            ZooUtils.createNode(zk, "/zhong/aaa", "zhong", ZooDefs.Ids.CREATOR_ALL_ACL);


            System.err.println("创建成功");
        } finally {
            zk.close();
        }
    }

    @Test
    public void getData() throws KeeperException, InterruptedException {
        try {
            Stat stat = new Stat();
            byte[] data = ZooUtils.getData(zk, "/zhong", stat);
            System.err.println(new String(data));
            System.err.println("节点数据的版本号：" + stat.getVersion());
        } finally {
            zk.close();
        }
    }

    @Test
    public void updateData() throws KeeperException, InterruptedException {

        try {
            ZooUtils.updateData(zk, "/zhong", "message", 0);
            System.err.println("更新成功");
        } finally {
            zk.close();
        }
    }

    @Test
    public void getACL() throws KeeperException, InterruptedException {
        try {
            List<ACL> acls = ZooUtils.getACL(zk, "/zhong/yun", new Stat());
            for (ACL acl :
                    acls) {
                System.err.println(acl.getId() + ":" + acl.getPerms());
            }
        } finally {
            zk.close();
        }
    }

    @Test
    public void getChildren() throws KeeperException, InterruptedException {
        try {
            List<String> children = ZooUtils.getChildren(zk, "/kafka110");
            for (String child :
                    children) {
                System.err.println(child);
            }
        } finally {
            zk.close();
        }
    }

    @Test
    public void deleteNode() throws KeeperException, InterruptedException {
        try {
            ZooUtils.deleteNode(zk, "/zhong/aaa", 0);
            System.err.println("删除成功");
        } finally {
            zk.close();
        }
    }
}