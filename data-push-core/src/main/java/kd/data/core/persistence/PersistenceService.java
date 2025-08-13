package kd.data.core.persistence;

import java.util.List;

/**
 * 持久化
 *
 * @author gaozw
 * @date 2025/8/11 16:38
 */
@SuppressWarnings("unused")
public interface PersistenceService<T> {

    void persist(T model);
    void persistBatch(List<T> models);
    void shutdown();
}
