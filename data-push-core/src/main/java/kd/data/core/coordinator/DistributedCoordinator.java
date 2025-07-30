package kd.data.core.coordinator;

/**
 * 分布式协调器接口
 * @author gaozw
 * @date 2025/7/23 18:09
 */
@SuppressWarnings("unused")
public interface DistributedCoordinator {

    /**
     * 尝试获取分布式锁
     * @param lockKey 锁的键
     * @param timeoutSeconds 锁的超时时间(秒)
     * @return 是否获取成功
     */
    boolean tryLock(String lockKey, int timeoutSeconds);

    /**
     * 释放分布式锁
     * @param lockKey 锁的键
     */
    void unlock(String lockKey);

    /**
     * 保存检查点
     * @param key 检查点键
     * @param checkpoint 检查点值
     */
    void saveCheckpoint(String key, String checkpoint);

    /**
     * 加载检查点
     * @param key 检查点键
     * @return 检查点值
     */
    String loadCheckpoint(String key);

    /**
     * 删除检查点
     * @param key 检查点键
     */
    default void deleteCheckpoint(String key) {
        // 默认空实现
    }

    default boolean renewLock(String lockKey, int timeoutSeconds) { return false; }

    /**
     * 监听分片状态变化
     * @param shardId 分片ID
     * @param listener 监听器
     */
    default void watchShardStatus(int shardId, ShardStatusListener listener) {
        // 默认空实现
    }

    /**
     * 未监听分片状态
     * @param shardId 分片ID
     */
    default void unWatchShardStatus(int shardId) {}

    /**
     * 分片状态监听器
     */
    interface ShardStatusListener {
        void onShardStatusChanged(int shardId, String status);
    }
}
