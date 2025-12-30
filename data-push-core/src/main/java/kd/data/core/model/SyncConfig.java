package kd.data.core.model;

import lombok.Data;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * 配置模型
 * @author gaozw
 * @date 2025/7/23 17:21
 */
@Component
@Data
public class SyncConfig implements Serializable {

    // 线程池配置
    private int corePoolSize = Runtime.getRuntime().availableProcessors();
    private int maxPoolSize = corePoolSize * 4;
    private int queueCapacity = 100_000;

    // 批处理配置
    private int batchSize = 5_000;
    private int maxRetries = 3;
    private long retryInitialDelay = 1000;
    private double retryBackoffFactor = 2.0;
    private boolean idempotentMode = true;

    // 分片配置
    private int minShards = 1;
    private int maxShards = 64;
    private long recordsPerShard = 1_000_000L;

    // 超时配置
    private int lockTimeout = 1000;
    private int lockRenewInterval = 30; // 新增锁续期间隔
    private int shardTimeout = 3600;
    private int globalTimeout = 24 * 3600;

    // 协调器配置
    private String coordinatorType = "redis";
    private Map<String, String> coordinatorConfig = new HashMap<>();

    // 性能优化
    private boolean useCompression = false;

    private int compressionThreshold = 10_000;

    // 监控配置
    private int statsInterval = 5;


    // JDBC特有配置
    private String countQuery;
    private String selectQuery;

}
