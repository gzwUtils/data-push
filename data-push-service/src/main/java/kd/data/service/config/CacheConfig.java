package kd.data.service.config;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import kd.data.core.model.SyncStats;
import kd.data.service.model.SyncTaskConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

@Slf4j
@Configuration
public class CacheConfig {

    @Bean
    public Cache<String, SyncTaskConfig> taskCache() {
        return Caffeine.newBuilder()
                .expireAfterWrite(7, TimeUnit.DAYS) // 写入后过期
                .maximumSize(10_000)                 // 降低最大容量
                .recordStats()                       // 开启统计
                .removalListener((key, value, cause) ->
                        log.debug("任务[{}]被移除，原因: {}", key, cause)
                )
                .build();
    }

    // 新增：已完成任务统计信息的缓存（保留3天）
    @Bean("completedTasksCache")
    public Cache<String, SyncStats> completedTasksCache() {
        return Caffeine.newBuilder()
                .expireAfterWrite(3, TimeUnit.DAYS)  // 3天后自动过期
                .maximumSize(10_000)                 // 最大缓存数量
                .recordStats()                       // 开启统计
                .removalListener((String key, SyncStats stats, RemovalCause cause) -> {
                    if (stats != null) {
                        log.info("任务统计[{}]已过期移除, 状态: {}, 原因: {}",
                                key, stats.getStatus(), cause);
                    } else {
                        log.debug("空任务统计[{}]被移除, 原因: {}", key, cause);
                    }
                })
                .build();
    }
}
