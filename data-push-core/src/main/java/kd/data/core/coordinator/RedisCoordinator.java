package kd.data.core.coordinator;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import kd.data.core.exception.SyncException;
import lombok.extern.slf4j.Slf4j;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 基于Redis的分布式协调器实现
 * @author gaozw
 * @date 2025/7/23 17:49
 */
@Slf4j
@SuppressWarnings("unused")
public class  RedisCoordinator implements DistributedCoordinator,AutoCloseable{
    private static final String LOCK_PREFIX     = "sync:redis:lock:";
    private static final String CHECKPOINT_PREFIX = "sync:redis:checkpoint:";

    private static final String NODE_SEPARATOR = "[,，;；:：]";


    //使用Object类型，可以持有单机或集群客户端
    private final Object redisClient;
    private final Object connection;
    private final boolean isCluster;
    private final RedisCommands<String, String> syncCmd;
    private final RedisAdvancedClusterCommands<String, String> clusterCmd;

    /**
     * 单机Redis构造器
     * @param host Redis主机地址
     * @param port Redis端口
     * @param password Redis密码（可为null）
     */
    @SuppressWarnings("unchecked")
    public RedisCoordinator(String host, int port, String password) {
        this.isCluster = false;

        // 构建RedisURI
        RedisURI.Builder uriBuilder = RedisURI.builder()
                .withHost(host)
                .withPort(port)
                .withTimeout(Duration.ofSeconds(10));

        if (password != null && !password.trim().isEmpty()) {
            uriBuilder.withPassword(password.toCharArray());
        }

        RedisURI redisUri = uriBuilder.build();

        // 创建单机客户端和连接
        this.redisClient = RedisClient.create(redisUri);
        this.connection = ((RedisClient) redisClient).connect();
        this.syncCmd = ((StatefulRedisConnection<String, String>) connection).sync();
        this.clusterCmd = null;
    }

    /**
     * 集群Redis构造器
     * @param nodes Redis集群节点列表（格式: host:port）
     * @param password Redis密码（可为null）
     */
    @SuppressWarnings("unchecked")
    public RedisCoordinator(List<String> nodes, String password) {
        this.isCluster = true;

        // 构建RedisURI列表
        List<RedisURI> redisUris = nodes.stream().map(node -> {
            String[] parts = node.split(NODE_SEPARATOR);
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);

            RedisURI.Builder uriBuilder = RedisURI.builder()
                    .withHost(host)
                    .withPort(port)
                    .withTimeout(Duration.ofSeconds(10));

            if (password != null && !password.trim().isEmpty()) {
                uriBuilder.withPassword(password.toCharArray());
            }

            return uriBuilder.build();
        }).collect(Collectors.toList());

        // 创建集群客户端和连接
        this.redisClient = RedisClusterClient.create(redisUris);
        this.connection = ((RedisClusterClient) redisClient).connect();
        this.clusterCmd = ((StatefulRedisClusterConnection<String, String>) connection).sync();
        this.syncCmd = null;
    }

    /**
     * 便捷构造器 - 单机Redis（无密码，默认数据库）
     */
    public RedisCoordinator(String host, int port) {
        this(host, port, null);
    }

    /**
     * 便捷构造器 - 集群Redis（无密码）
     */
    public RedisCoordinator(List<String> nodes) {
        this(nodes, null);
    }


    @Override
    public boolean tryLock(String lockKey, int timeoutSeconds) {
        String key = LOCK_PREFIX + lockKey;
        try {
            if (isCluster) {
                return "OK".equals(clusterCmd.set(key, "locked",
                        SetArgs.Builder.nx().ex(timeoutSeconds)));
            } else {
                return "OK".equals(syncCmd.set(key, "locked",
                        SetArgs.Builder.nx().ex(timeoutSeconds)));
            }
        } catch (Exception e) {
            // 记录日志
            log.error("redis tryLock error key {} {}",key,e.getMessage(),e);
            return false;
        }
    }

    @Override
    public void unlock(String lockKey) {
        String key = LOCK_PREFIX + lockKey;
        try {
            if (isCluster) {
                clusterCmd.del(key);
            } else {
                syncCmd.del(key);
            }
        } catch (Exception e) {
            // 记录日志
        }
    }

    @Override
    public void saveCheckpoint(String key, String checkpoint) {
        try {
            if (isCluster) {
                clusterCmd.set(CHECKPOINT_PREFIX + key, checkpoint);
            } else {
                syncCmd.set(CHECKPOINT_PREFIX + key, checkpoint);
            }
        } catch (Exception e) {
            log.error("redis saveCheckpoint error key {} {}",key,e.getMessage(),e);
            throw new SyncException("Failed to save checkpoint", e);
        }
    }

    @Override
    public String loadCheckpoint(String key) {
        try {
            if (isCluster) {
                return clusterCmd.get(CHECKPOINT_PREFIX + key);
            } else {
                return syncCmd.get(CHECKPOINT_PREFIX + key);
            }
        } catch (Exception e) {
            log.error("redis loadCheckpoint error key {} {}",key,e.getMessage(),e);
            throw new SyncException("Failed to load checkpoint", e);
        }
    }

    @Override
    public void deleteCheckpoint(String key) {
        try {
            if (isCluster) {
                clusterCmd.del(CHECKPOINT_PREFIX + key);
            } else {
                syncCmd.del(CHECKPOINT_PREFIX + key);
            }
        } catch (Exception e) {
            log.error("redis deleteCheckpoint error key {} {}",key,e.getMessage(),e);
            throw new SyncException("Failed to delete checkpoint", e);
        }
    }

    @Override
    public boolean renewLock(String lockKey, int timeoutSeconds) {
        String key = LOCK_PREFIX + lockKey;
        try {
            if (isCluster) {
                return clusterCmd.expire(key, timeoutSeconds);
            } else {
                return syncCmd.expire(key, timeoutSeconds);
            }
        } catch (Exception e) {
            log.error("redis renewLock error {}",e.getMessage(),e);
            return false;
        }
    }

    /** 关闭资源 */
    @Override
    public void close() {
        try {
            if (isCluster) {
                ((StatefulRedisClusterConnection<?, ?>) connection).close();
                ((RedisClusterClient) redisClient).shutdown();
            } else {
                ((StatefulRedisConnection<?, ?>) connection).close();
                ((RedisClient) redisClient).shutdown();
            }
        } catch (Exception e) {
                log.error("redis close error {}",e.getMessage(),e);
        }
    }
}
