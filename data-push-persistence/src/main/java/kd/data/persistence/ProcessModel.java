package kd.data.persistence;
import kd.data.core.model.SyncConfig;
import kd.data.service.entity.FieldMapping;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * 持久化配置
 *
 * @author gaozw
 * @date 2025/8/8 14:05
 */
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Getter
public class ProcessModel extends BaseModel {


    /**任务id*/
    private String taskId;

    /**任务名称*/
    private String taskName;

    /**原类型*/
    private String sourceType;

    private String destinationType;

    /**源配置*/
    private Map<String,Object> sourceConfig;

    /**目标配置*/
    private Map<String,Object> desConfig;

    /**核心配置*/
    private SyncConfig syncConfig;

    /**源名称*/
    private String sourceFieldName;
    /**目标名称*/
    private String targetFieldName;

    /** 字段*/
    private List<FieldMapping> fields;





}
