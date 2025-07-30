package kd.data.web.vo;

import kd.data.core.model.SyncConfig;
import lombok.Data;

import java.util.Map;

/**
 * @author gaozw
 * @date 2025/7/25 17:51
 */
@Data
public class TaskRequest {

    private String taskId;
    private String taskName;
    private String sourceType;
    private Map<String, Object> sourceConfig;
    private Map<String, Object> destinationConfig;
    private SyncConfig syncConfig;
    private String sourceEntityClassName; // 源实体类全名
    private String targetEntityClassName; // 目标实体类全名
}
