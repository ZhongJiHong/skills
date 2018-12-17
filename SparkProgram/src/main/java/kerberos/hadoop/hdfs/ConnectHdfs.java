package kerberos.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @Author G.Goe
 * @Date 2018/12/10
 * @Request
 * @Resource
 */
public class ConnectHdfs {

    private static final Logger logger = LoggerFactory.getLogger(ConnectHdfs.class);

    static {
        System.setProperty("java.security.krb5.conf", "D://admin//data//krb5.conf");
        System.setProperty("sun.security.krb5.debug", "true");
    }

    public static void main(String[] args) {

        String nameServices = "beh001";
        String bootstrap = "hadoop001:9000,hadoop002:9000";

        String userPrincipal = "hadoop/hadoop001@BONC";
        String keytabPath = "D:/admin/data/krb5.keytab";
        String namenodePrincipal = "nn/_HOST@BONC";
        String datanodePrincipal = "dn/_HOST@BONC";

        Configuration configuration = HdfsConfigFactory.getConfigurationForKerberos(
                nameServices, bootstrap, namenodePrincipal, datanodePrincipal);

        try (DistributedFileSystem dfs = new DistributedFileSystem()) {
            // loggin in -- 必须在初始化FileSystem之前，否则无法FileSystem初始化失败
            UserGroupInformation.setConfiguration(configuration);
            UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(userPrincipal, keytabPath);
            UserGroupInformation.setLoginUser(ugi);

            dfs.initialize(new URI(configuration.get("fs.defaultFS")), configuration);

            // TODO -- HDFS Operation of CRUD
//            boolean result = dfs.delete(new Path("/zhong/dump/TestTable_dump2"), true);
//            logger.info("The resource has been deleted! {}", result);

//            boolean result = dfs.mkdirs(new Path("/zhong/test/tmp"),FsPermission.getFileDefault());
//            logger.info("The dir has been created!{}",result);

            /*DistributedFileSystem.HdfsDataOutputStreamBuilder streamBuilder = dfs.createFile(new Path("/zhong/test/tmp/info.log"));
            FSDataOutputStream build = streamBuilder.blockSize(5120000).permission(FsPermission.getFileDefault()).build();
            build.flush();
            build.close();*/

            /*FSDataOutputStream outputStream = dfs.append(new Path("/zhong/test/tmp/info.log"));
            try (FileInputStream inputStream = new FileInputStream("D:\\admin\\data\\component-info.log.2018-07-31")) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                String value = null;
                while((value=reader.readLine())!=null){
                    outputStream.write(value.getBytes());
                }
            }
            outputStream.close();*/

            /*BlockLocation[] locations = dfs.getFileBlockLocations(new Path("/zhong/test/tmp/info.log"), 0, Integer.MAX_VALUE);
            Arrays.asList(locations).forEach(item->{
                try {
                    System.err.println(item.toString());
                    System.err.println(Arrays.toString(item.getHosts()));
                    System.err.println(Arrays.toString(item.getNames()));
                    System.err.println(item.getLength());
                    System.err.println(Arrays.toString(item.getTopologyPaths()));
                } catch (IOException e) {
                    logger.error(e.getMessage());
                }
            });*/

        } catch (URISyntaxException | IOException e) {
            logger.error(e.getMessage());
        }
    }
}
