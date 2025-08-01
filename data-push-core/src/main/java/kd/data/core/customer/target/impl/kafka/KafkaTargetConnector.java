package kd.data.core.customer.target.impl.kafka;

import kd.data.core.customer.target.TargetConnector;
import kd.data.core.customer.target.targetenums.TargetEnums;
import kd.data.core.exception.SyncException;
import lombok.Getter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
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

    public KafkaTargetConnector(String bootstrapServers) {
        this.kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
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
