package kd.data.core.customer.target.impl.kafka;

import kd.data.core.customer.meta.ConsumerMetadata;
import kd.data.core.customer.target.TargetConnector;
import kd.data.core.customer.target.TargetWriter;
import kd.data.core.exception.SyncException;
import kd.data.core.utils.ReflectionUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * kafka 适配器
 *
 * @author gaozw
 * @date 2025/8/1 11:22
 */
@SuppressWarnings("unused")
public class KafkaTargetWriter<T> implements TargetWriter<T> {

    private final Function<T, String> keyExtractor;
    private final ObjectMapper objectMapper = new ObjectMapper(); // JSON序列化器

    public KafkaTargetWriter(Function<T, String> keyExtractor) {
        this.keyExtractor = keyExtractor;
    }

    @Override
    public void write(List<T> batch, TargetConnector connector, ConsumerMetadata metadata) {
        if (!(connector instanceof KafkaTargetConnector)) {
            throw new IllegalArgumentException("Kafka写入器需要Kafka连接器");
        }

        Producer<String, byte[]> producer = ((KafkaTargetConnector) connector).getProducer();
        String targetTopic = metadata.getTopicName();

        for (T item : batch) {
            try {
                // 1. 创建映射文档（应用字段映射）
                Map<String, Object> document = createMappedDocument(item, metadata);

                // 2. 转换为JSON字节
                byte[] jsonBytes = objectMapper.writeValueAsBytes(document);

                // 3. 提取键值
                String key = keyExtractor.apply(item);

                // 4. 发送到Kafka
                ProducerRecord<String, byte[]> re = new ProducerRecord<>(targetTopic, key, jsonBytes);
                producer.send(re);
            } catch (Exception e) {
                throw new SyncException("Kafka写入失败: " + e.getMessage(), e);
            }
        }

        // 5. 确保所有消息发送完成
        producer.flush();
    }

    /**
     * 创建映射文档 - 应用字段映射规则
     */
    private Map<String, Object> createMappedDocument(T item, ConsumerMetadata metadata) {
        Map<String, Object> document = new HashMap<>();

        // 遍历所有字段定义
        for (ConsumerMetadata.ConsumerFieldModel field : metadata.getFields()) {
            try {
                // 获取字段值
                Object value = ReflectionUtils.getFieldValue(item, field.getFieldName());

                // 确定目标字段名
                String targetName = field.getTargetName();
                if (targetName == null || targetName.isEmpty()) {
                    targetName = field.getFieldName(); // 使用原始字段名
                }

                // 添加到文档
                document.put(targetName, value);
            } catch (Exception e) {
                throw new SyncException("字段映射失败: " + field.getFieldName(), e);
            }
        }

        return document;
    }
}
