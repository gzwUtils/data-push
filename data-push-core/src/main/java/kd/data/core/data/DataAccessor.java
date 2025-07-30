package kd.data.core.data;

import kd.data.core.model.SyncConfig;

import java.io.Closeable;

/**
 * @author gaozw
 * @date 2025/7/23 18:15
 */

public interface DataAccessor<T> extends Closeable {

    void init(SyncConfig config);

    long countRecords();

    Cursor<T> openCursor(int shardId, int totalShards, String checkpoint);

    String getRecordId(T re);

    String getFilterKey(T re);

    /**
     * 返回当前分片的最大 id（用于断点续传）
     */
    String getMaxCheckpointInShard(int shardId, int totalShards);


    @Override
    default void close() {}

    interface Cursor<T> extends Closeable {
        boolean hasNext();
        T next();

        @Override
        default void close() {}
    }
}
