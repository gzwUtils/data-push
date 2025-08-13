package kd.data.service.config;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
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
                .expireAfterWrite(2, TimeUnit.HOURS) // 写入后过期
                .maximumSize(10_000)                 // 降低最大容量
                .recordStats()                       // 开启统计
                .removalListener((key, value, cause) ->
                        log.debug("任务[{}]被移除，原因: {}", key, cause)
                )
                .build();
    }
}
