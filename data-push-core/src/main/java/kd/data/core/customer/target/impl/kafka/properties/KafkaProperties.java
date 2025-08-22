package kd.data.core.customer.target.impl.kafka.properties;

import lombok.Data;

/**
 * kafka属性
 *
 * @author gaozw
 * @date 2025/8/22 10:42
 */
@Data
public class KafkaProperties {



    private KafkaProperties(){}

    /* ========== 必填 ========== */
    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";

    /* ========== Producer 可选 ========== */
    public static final String PRODUCER_BATCH_SIZE      = "producer.batch.size";
    public static final String PRODUCER_LINGER_MS       = "producer.linger.ms";
    public static final String PRODUCER_ACKS            = "producer.acks";
    public static final String PRODUCER_RETRIES         = "producer.retries";
    public static final String PRODUCER_ENABLE_IDEMPOTENCE = "producer.enable.idempotence";

    /* ========== 安全 ========== */
    public static final String SECURITY_PROTOCOL       = "security.protocol";
    public static final String SASL_MECHANISM          = "sasl.mechanism";
    public static final String SASL_JAAS_CONFIG        = "sasl.jaas.config";
    public static final String SSL_TRUSTSTORE_LOCATION = "ssl.truststore.location";
    public static final String SSL_TRUSTSTORE_PASSWORD = "ssl.truststore.password";
    public static final String SSL_KEYSTORE_LOCATION   = "ssl.keystore.location";
    public static final String SSL_KEYSTORE_PASSWORD   = "ssl.keystore.password";
    public static final String SSL_ENDPOINT_ID_ALGORITHM = "ssl.endpoint.identification.algorithm";

    /* ========== 默认值 ========== */
    public static final String DEFAULT_BATCH_SIZE      = "16384";
    public static final String DEFAULT_LINGER_MS       = "5";
    public static final String DEFAULT_ACKS            = "all";
    public static final String DEFAULT_RETRIES         = "3";
    public static final String DEFAULT_ENABLE_IDEMPOTENCE = "true";
}
