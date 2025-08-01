package kd.data.service.datasource;

import kd.data.core.customer.BatchConsumerService;
import kd.data.core.customer.meta.ConsumerMetadata;
import kd.data.core.customer.target.TargetConnector;
import kd.data.core.customer.target.TargetWriter;
import kd.data.core.customer.target.impl.jdbc.JdbcBatchWriter;
import kd.data.core.customer.target.impl.jdbc.JdbcTargetConnector;
import kd.data.core.customer.target.targetenums.TargetEnums;
import org.springframework.stereotype.Component;
import javax.sql.DataSource;
import java.util.Map;

/**
 * @author gaozw
 * @date 2025/7/25 16:54
 */
@Component
public class ConsumerDataAccessFactoryManager {



    public <T> BatchConsumerService<T> createConsumer(
            Class<T> entityType,
            Map<String, Object> config) {

        BatchConsumerService<T> consumer = new BatchConsumerService<>(entityType);
        ConsumerMetadata metadata = consumer.getMetadata();

        // 获取目标类型字符串
        String targetType = metadata.getTargetType();
        // 根据目标类型注册连接器和写入器
        TargetEnums targetEnum = TargetEnums.getTargetEnum(targetType);

        if(targetEnum == null){
            throw new IllegalArgumentException("不支持的目标类型: " + metadata.getTargetType());
        }
        switch (targetEnum) {
            case JDBC:
                registerJdbcTarget(consumer, DataSourceManager.getJdbcDataSource(config));
                break;
            case ELASTICSEARCH:
                registerEsTarget(consumer, config);
                break;
            case KAFKA:
                registerKafkaTarget(consumer, config);
                break;
            default:
                throw new IllegalArgumentException("不支持的目标类型: " + metadata.getTargetType());
        }

        return consumer;
    }

    private <T> void registerJdbcTarget(
            BatchConsumerService<T> consumer,
            DataSource dataSource) {

        TargetConnector connector = new JdbcTargetConnector(dataSource);
        TargetWriter<T> writer = new JdbcBatchWriter<>();

        consumer.registerTarget(TargetEnums.JDBC.name(), connector);
        consumer.registerWriter(TargetEnums.JDBC.name(), writer);
    }

    @SuppressWarnings("unused")
    private <T> void registerEsTarget(
            BatchConsumerService<T> consumer,
            Map<String, Object> config) {

        // 创建ES连接器
    }

    @SuppressWarnings("unused")
    private <T> void registerKafkaTarget(
            BatchConsumerService<T> consumer,
            Map<String, Object> config) {

        // 创建Kafka连接器
    }
}
