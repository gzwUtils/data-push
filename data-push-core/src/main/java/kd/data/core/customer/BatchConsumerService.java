package kd.data.core.customer;

import kd.data.core.customer.meta.ConsumerMetadata;
import kd.data.core.customer.target.TargetConnector;
import kd.data.core.customer.target.TargetWriter;
import kd.data.core.exception.SyncException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 *
 * 通用批处理消费者服务
 * 支持多目标路由、数据转换、异常处理
 * @author gaozw
 * @date 2025/7/23 17:57
 */
@SuppressWarnings("unused")
@Slf4j
public class BatchConsumerService<T> implements Consumer<List<T>> {

    @Getter
    private final ConsumerMetadata metadata;
    private final Map<String, TargetConnector> connectors = new HashMap<>();
    private final Map<String, TargetWriter<T>> writers = new HashMap<>();

    public BatchConsumerService(Class<T> entityType) {
        this.metadata = new ConsumerMetadata(entityType);
    }

    @Override
    public void accept(List<T> batch) {
        writers.forEach((targetId, writer) -> {
            try {
                TargetConnector connector = connectors.get(targetId);
                if (connector == null) {
                    throw new SyncException("No connector for target: " + targetId);
                }
                writer.write(batch, connector, metadata);
            } catch (Exception e) {
                log.error("Failed to write batch to {}: {}", targetId, e.getMessage(),e);
                throw new SyncException("Failed to write batch to "+ targetId ,e);
            }
        });
    }

    public void registerTarget(String targetId, TargetConnector connector) {
        connectors.put(targetId, connector);
    }

    public void registerWriter(String targetId, TargetWriter<T> writer) {
        writers.put(targetId, writer);
    }

}
