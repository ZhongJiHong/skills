package kerberos.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.security.access.Permission;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @Author G.Goe
 * @Date 2018/9/26
 * @Request JDK1.8及以上版本
 * @Resource
 */
public class HbaseUtilsTest {

    private Connection connection = null;
    private Admin admin = null;

    @Before
    public void initEnv() throws IOException {

        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop001,hadoop002,hadoop003");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        connection = HbaseUtils.getConnection("hadoop/hadoop001@BONC",
                "D:\\admin\\data\\krb5.keytab");
//        connection = ConnectionFactory.createConnection(configuration);
        admin = connection.getAdmin();
    }

    @Test
    public void createOrOverwrite() {
        try {
            HbaseUtils.createOrOverwrite(admin, "aaabbb", "aaa", "bbb", "ccc");  // 创建表
            // deleteTable(admin, "user");  // 删除表
            admin.close();
        } catch (IOException e) {
            System.out.println("Table Created Failed!");
        } finally {
            HbaseUtils.close(connection, admin);
        }
    }

    @Test
    public void deleteTable() {
        try {
            HbaseUtils.deleteTable(admin, "aaabbb");  // 创建表
            // deleteTable(admin, "user");  // 删除表
            admin.close();
        } catch (IOException e) {
            System.out.println("Table Deleted Failed!");
        } finally {
            HbaseUtils.close(connection, admin);
        }
    }

    @Test
    public void addPut() {
        try {
            Put put = new Put("12306".getBytes());    // 插入数据
            put.addColumn("ccc".getBytes(), "c1".getBytes(), "fuwu".getBytes());
            // put.addColumn("bbb".getBytes(), "b1".getBytes(), "goupiao".getBytes());
            HbaseUtils.addPut(connection, "user", put);
        } catch (IOException e) {
            System.out.println("Put Created Failed!");
        } finally {
            HbaseUtils.close(connection, admin);
        }
    }

    @Test
    public void getData() {
        try {
            String data = HbaseUtils.getData(connection, "test", "row1", "cf", "info1");  // 获取数据
            System.err.println("----------------" + data + "-------------------");
        } catch (IOException e) {
            System.out.println("Data Obtained Failed!");
        } finally {
            HbaseUtils.close(connection, admin);
        }
    }

    @Test
    public void deletePut() {
        try {
            HbaseUtils.deletePut(connection, "user", "12306");
        } catch (IOException e) {
            System.out.println("Put Deleted Failed!");
        } finally {
            HbaseUtils.close(connection, admin);
        }
    }

    @Test
    public void grantOnTable() {
        try {
            HbaseUtils.grantOnTable(connection, "user", "easy02", "aaa", "a1", Permission.Action.READ, Permission.Action.WRITE); //授予权限
        } catch (Throwable throwable) {
            System.out.println("Granted Failed!");
        } finally {
            HbaseUtils.close(connection, admin);
        }
    }

    @Test
    public void revokeOnTable() {
        try {
            HbaseUtils.revokeOnTable(connection, "user", "easy02", "aaa", "a1", Permission.Action.READ, Permission.Action.WRITE); //授予权限
        } catch (Throwable throwable) {
            System.out.println("Revoked Failed!");
        } finally {
            HbaseUtils.close(connection, admin);
        }
    }

    @Test
    public void listTables() {
        try {
            NamespaceDescriptor[] namespace = admin.listNamespaceDescriptors();
            for (NamespaceDescriptor name : namespace) {
                TableName[] tables = admin.listTableNamesByNamespace(name.getName());
                for (TableName table : tables) {
                    System.err.println(table.getNameAsString());
                }
            }
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}