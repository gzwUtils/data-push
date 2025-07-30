package kd.data.core.customer.target;

import kd.data.core.customer.meta.ConsumerMetadata;

import java.util.List;

/**
 * 目标写入器接口
 * @author gaozw
 * @date 2025/7/24 16:42
 */
@SuppressWarnings("unused")
public interface TargetWriter<T> {

    void write(List<T> batch, TargetConnector connector, ConsumerMetadata metadata);
}
