package kd.data.core.coordinator;
import io.lettuce.core.RedisClient;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
/**
 * 基于Redis的分布式协调器实现
 * @author gaozw
 * @date 2025/7/23 17:49
 */

@SuppressWarnings("unused")
public class  RedisCoordinator implements DistributedCoordinator{
    private static final String LOCK_PREFIX     = "sync:lock:";
    private static final String CHECKPOINT_PREFIX = "sync:checkpoint:";

    private final RedisClient             redisClient;
    private final StatefulRedisConnection<String, String> connection;
    private final RedisCommands<String, String> syncCmd;

    public RedisCoordinator(String host, int port) {
        this.redisClient = RedisClient.create("redis://" + host + ":" + port);
        this.connection  = redisClient.connect();
        this.syncCmd     = connection.sync();
    }

    @Override
    public boolean tryLock(String lockKey, int timeoutSeconds) {
        String key = LOCK_PREFIX + lockKey;
        return "OK".equals(syncCmd.set(key, "locked",
                SetArgs.Builder.nx().ex(timeoutSeconds)));
    }

    @Override
    public void unlock(String lockKey) {
        syncCmd.del(LOCK_PREFIX + lockKey);
    }

    @Override
    public void saveCheckpoint(String key, String checkpoint) {
        syncCmd.set(CHECKPOINT_PREFIX + key, checkpoint);
    }

    @Override
    public String loadCheckpoint(String key) {
        return syncCmd.get(CHECKPOINT_PREFIX + key);
    }

    @Override
    public void deleteCheckpoint(String key) {
        syncCmd.del(CHECKPOINT_PREFIX + key);
    }

    @Override
    public boolean renewLock(String lockKey, int timeoutSec) {
        return syncCmd.expire(LOCK_PREFIX + lockKey, timeoutSec);
    }

    /** 关闭资源 */
    public void close() {
        connection.close();
        redisClient.shutdown();
    }
}
