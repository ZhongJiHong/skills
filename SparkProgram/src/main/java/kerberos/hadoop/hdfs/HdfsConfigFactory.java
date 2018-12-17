package kerberos.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;

/**
 * @Author G.Goe
 * @Date 2018/12/14
 * @Request
 * @Resource
 */
public class HdfsConfigFactory {

    private HdfsConfigFactory() {
    }

    /**
     * Ha HDFS基础Kerberos配置
     *
     * @param nameServices
     * @param HaBootstrap
     * @return
     */
    public static Configuration getConfigurationForKerberos(String nameServices, String HaBootstrap,
                                                            String namenodePrincipal, String datanodePrincipal) {

        Configuration configuration = new Configuration(false);
        configuration.set("fs.defaultFS", "hdfs://" + nameServices);
        configuration.set("dfs.nameservices", nameServices);
        configuration.set("dfs.ha.namenodes." + nameServices, "nn1,nn2");
        configuration.set("dfs.namenode.rpc-address." + nameServices + ".nn1", HaBootstrap.split(",")[0]);
        configuration.set("dfs.namenode.rpc-address." + nameServices + ".nn1", HaBootstrap.split(",")[1]);
        configuration.set("dfs.client.failover.proxy.provider." + nameServices, "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

        configuration.set("hadoop.security.authentication", "kerberos");
        // TODO -- 根据实际配置情况及使用模式进行配置
//        configuration.set("hadoop.rpc.protection", "privacy");
        configuration.set("dfs.data.transfer.protection", "authentication");
        configuration.set("dfs.namenode.kerberos.principal", namenodePrincipal);
        configuration.set("dfs.datanode.kerberos.principal", datanodePrincipal);

        return configuration;
    }
}
