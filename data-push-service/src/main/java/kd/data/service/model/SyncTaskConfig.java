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

    private String taskId;

    private String taskName;

    private String sourceType; // mysql,oracle

    private Map<String, Object> sourceConfig;

    private Map<String, Object> destinationConfig;

    private SyncConfig syncConfig; // 核心配置

    private boolean incremental; // 是否增量同步

    private String cronExpression; // 定时任务表达式
}
