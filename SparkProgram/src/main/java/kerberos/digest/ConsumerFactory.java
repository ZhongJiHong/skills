package kerberos.digest;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;

import java.util.Properties;

/**
 * @Author G.Goe
 * @Date 2018/10/29
 * @Request
 * @Resource
 */
public class ConsumerFactory {
    private ConsumerFactory() {
    }

    public static KafkaConsumer<byte[], byte[]> getConsumer(String bootstrap, String groupId, String clientId) {

//        System.setProperty("java.security.krb5.conf", "D:\\admin\\data\\krb5.conf");
//        System.setProperty("sun.security.krb5.debug", "true");
//        System.setProperty("java.security.auth.login.config", "D:\\admin\\data\\kafka_client_jaas.conf");
        System.setProperty("java.security.auth.login.config", "D:\\admin\\plain\\kafka_client_jaas.conf");

        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        // sasl 配置
        /*props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "GSSAPI");
        props.put("sasl.kerberos.service.name", "kafka");*/
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
//        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");

        // ssl 配置
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "D:\\admin\\plain\\client.keystore.jks");
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "kafka123");
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "D:\\admin\\plain\\client.truststore.jks");
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "kafka123");
        props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "kafka123");
//        props.put(SslConfigs.DEFAULT_SSL_TRUSTSTORE_TYPE, "JKS");

        return new KafkaConsumer<>(props);
    }
}
