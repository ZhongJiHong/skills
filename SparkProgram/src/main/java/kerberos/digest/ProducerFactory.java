package kerberos.digest;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;

import java.io.IOException;
import java.util.Properties;

/**
 * @Author G.Goe
 * @Date 2018/10/29
 * @Request
 * @Resource
 */
public class ProducerFactory {
    private ProducerFactory() {
    }

    public static KafkaProducer<byte[], byte[]> getProducer(String bootstrap, String clientId) throws IOException {

//        System.setProperty("java.security.krb5.conf", "D:\\admin\\data\\krb5.conf");
//        System.setProperty("sun.security.krb5.debug", "true");
//        System.setProperty("java.security.auth.login.config", "D:\\admin\\data\\kafka_client_jaas.conf");
        System.setProperty("java.security.auth.login.config", "D:\\admin\\plain\\kafka_client_jaas.conf");

        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "3000");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");

        // sasl 增加的配置
        /*props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "GSSAPI");
        props.put("sasl.kerberos.service.name", "kafka");*/
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
//        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");

        // ssl 需要增加的配置
        // 测试结果显示，此处keystore和truststore使用clinet和server均可
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "D:\\admin\\plain\\client.keystore.jks");
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "kafka123");
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "D:\\admin\\plain\\client.truststore.jks");
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "kafka123");
        props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "kafka123");
        props.put(SslConfigs.DEFAULT_SSL_TRUSTSTORE_TYPE, "JKS");

        return new KafkaProducer<>(props);
    }
}
