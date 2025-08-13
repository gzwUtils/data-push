package kd.data.core.persistence;

import java.util.List;

/**
 * 不持久化
 *
 * @author gaozw
 * @date 2025/8/11 17:06
 */
@SuppressWarnings("unused")
public class NoOpPersistenceService<T> implements PersistenceService<T> {
    @Override
    public void persist(T model) {
        // 空操作
    }

    @Override
    public void persistBatch(List<T> models) {
        // 空操作
    }

    @Override
    public void shutdown() {
        // 空操作
    }
}
