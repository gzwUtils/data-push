package kd.data.core.customer.target.impl.kafka;
import com.fasterxml.jackson.core.JsonProcessingException;
import kd.data.core.customer.meta.ConsumerMetadata;
import kd.data.core.customer.target.TargetConnector;
import kd.data.core.customer.target.TargetWriter;
import kd.data.core.exception.SyncException;
import kd.data.core.utils.ReflectionUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.RecordMetadata;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * kafka 适配器
 *
 * @author gaozw
 * @date 2025/8/1 11:22
 */
@Slf4j
@SuppressWarnings("unused")
public class KafkaTargetWriter<T> implements TargetWriter<T> {


    private final ObjectMapper objectMapper = new ObjectMapper();

    private final boolean enableAsync;
    private final long timeoutMs;

    public KafkaTargetWriter() {
        this(true, 5000);
    }

    public KafkaTargetWriter(boolean enableAsync, long timeoutMs) {
        this.enableAsync = enableAsync;
        this.timeoutMs = timeoutMs;
    }

    @Override
    public void write(List<T> batch, TargetConnector connector, ConsumerMetadata metadata) {
        if (!(connector instanceof KafkaTargetConnector)) {
            throw new IllegalArgumentException("Kafka写入器需要Kafka连接器");
        }

        Producer<String, byte[]> producer = ((KafkaTargetConnector) connector).getProducer();
        String targetTopic = metadata.getTopicName();

        if (batch == null || batch.isEmpty()) {
            log.warn("尝试写入空批次数据到主题: {}", targetTopic);
            return;
        }

        write(batch, metadata, targetTopic, producer);
    }

    private void write(List<T> batch, ConsumerMetadata metadata, String targetTopic, Producer<String, byte[]> producer) {
        try {
            for (T item : batch) {
                // 1. 创建映射文档（应用字段映射）
                Map<String, Object> document = createMappedDocument(item, metadata);

                // 2. 转换为JSON字节
                byte[] jsonBytes = objectMapper.writeValueAsBytes(document);

                // 3. 提取键值
                String keyField = metadata.getKeyField();
                String key = (keyField == null) ? null
                        : String.valueOf(ReflectionUtils.getFieldValue(item, keyField));

                // 4. 发送到Kafka
                ProducerRecord<String, byte[]> re = new ProducerRecord<>(targetTopic, key, jsonBytes);

                if (enableAsync) {
                    // 异步发送带回调
                    producer.send(re, (meta, exception) -> {
                        if (exception != null) {
                            log.error("发送消息到Kafka失败, topic: {}, key: {}", targetTopic, key, exception);
                        } else {
                            log.debug("消息发送成功, topic: {}, partition: {}, offset: {}",
                                    meta.topic(), meta.partition(), meta.offset());
                        }
                    });
                } else {
                    // 同步发送
                    Future<RecordMetadata> future = producer.send(re);
                    future.get(timeoutMs, TimeUnit.MILLISECONDS);
                }
            }

            // 5. 确保所有消息发送完成
            producer.flush();
            log.info("成功发送 {} 条消息到主题: {}", batch.size(), targetTopic);

        } catch (InterruptedException  | ExecutionException e) {
            log.error("kafka write error {}",e.getMessage(),e);
            Thread.currentThread().interrupt();
            throw new SyncException("Kafka写入失败: " + e.getMessage(), e);
        } catch (JsonProcessingException | TimeoutException e) {
            log.error("kafka write error {}",e.getMessage(),e);
            throw new SyncException("Kafka写入失败: " + e.getMessage(), e);
        }
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
