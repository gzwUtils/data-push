package kd.data.core.customer.target.impl.kafka;

import kd.data.core.customer.target.TargetConnector;
import kd.data.core.customer.target.impl.kafka.properties.KafkaProperties;
import kd.data.core.customer.target.targetenums.TargetEnums;
import kd.data.core.exception.SyncException;
import lombok.Getter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

/**
 * kafka
 *
 * @author gaozw
 * @date 2025/8/1 11:18
 */
@Getter
@SuppressWarnings("unused")
public class KafkaTargetConnector implements TargetConnector {


    private Producer<String, byte[]> producer;
    private boolean isConnected = false;
    private final Properties kafkaProps;

    public KafkaTargetConnector(Properties configProperties) {
        this.kafkaProps = createProducerProperties(configProperties);
    }

    /**
     * 创建生产者配置
     */
    private Properties createProducerProperties(Properties configProperties) {
        Properties props = new Properties();

        // 必需配置
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                configProperties.getProperty(KafkaProperties.BOOTSTRAP_SERVERS));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        // 可选配置 - 批量处理
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,
                configProperties.getProperty(KafkaProperties.PRODUCER_BATCH_SIZE, KafkaProperties.DEFAULT_BATCH_SIZE));
        props.put(ProducerConfig.LINGER_MS_CONFIG,
                configProperties.getProperty(KafkaProperties.PRODUCER_LINGER_MS, KafkaProperties.DEFAULT_LINGER_MS));

        // 可选配置 - 可靠性
        props.put(ProducerConfig.ACKS_CONFIG,
                configProperties.getProperty(KafkaProperties.PRODUCER_ACKS, KafkaProperties.DEFAULT_ACKS));
        props.put(ProducerConfig.RETRIES_CONFIG,
                configProperties.getProperty(KafkaProperties.PRODUCER_RETRIES, KafkaProperties.DEFAULT_RETRIES));
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                configProperties.getProperty(KafkaProperties.PRODUCER_ENABLE_IDEMPOTENCE, KafkaProperties.DEFAULT_ENABLE_IDEMPOTENCE));

        // 安全配置
        if ("SASL_SSL".equals(configProperties.getProperty(KafkaProperties.SECURITY_PROTOCOL))) {
            props.put("security.protocol", "SASL_SSL");
            props.put("sasl.mechanism", configProperties.getProperty(KafkaProperties.SASL_MECHANISM, "PLAIN"));
            props.put("sasl.jaas.config", configProperties.getProperty(KafkaProperties.SASL_JAAS_CONFIG));

            // SSL 配置
            props.put("ssl.truststore.location", configProperties.getProperty(KafkaProperties.SSL_TRUSTSTORE_LOCATION));
            props.put("ssl.truststore.password", configProperties.getProperty(KafkaProperties.SSL_TRUSTSTORE_PASSWORD));
            props.put("ssl.keystore.location", configProperties.getProperty(KafkaProperties.SSL_KEYSTORE_LOCATION));
            props.put("ssl.keystore.password", configProperties.getProperty(KafkaProperties.SSL_KEYSTORE_PASSWORD));
            props.put("ssl.endpoint.identification.algorithm",
                    configProperties.getProperty(KafkaProperties.SSL_ENDPOINT_ID_ALGORITHM, ""));
        }

        return props;
    }


    @Override
    public void connect() throws SyncException {
        try {
            this.producer = new KafkaProducer<>(kafkaProps);
            this.isConnected = true;
        } catch (Exception e) {
            throw new SyncException("Kafka connection failed", e);
        }
    }

    @Override
    public boolean isConnected() {
        return isConnected;
    }

    @Override
    public String getType() {
        return TargetEnums.KAFKA.name();
    }

    @Override
    public void close() {
        if (producer != null) {
            producer.close();
            isConnected = false;
        }
    }
}
