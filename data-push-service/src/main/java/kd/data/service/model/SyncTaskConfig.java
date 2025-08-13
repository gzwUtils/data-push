package kd.data.service.model;

import kd.data.core.model.SyncConfig;
import lombok.Data;
import java.util.Map;
/**
 * @author gaozw
 * @date 2025/7/25 10:39
 */
@Data
public class SyncTaskConfig {
    /**任务id*/
    private String taskId;
    /**任务名称*/
    private String taskName;
    /**源配置类型*/
    private String sourceType; // mysql,oracle
    /**源配置*/
    private Map<String, Object> sourceConfig;
    /**核心配置*/
    private Map<String, Object> destinationConfig;
    /**核心配置*/
    private SyncConfig syncConfig;
    /**是否增量同步*/
    private boolean incremental;
    /**定时任务表达式*/
    private String cronExpression;

    /**是否已持久化*/
    private boolean persist;
}
