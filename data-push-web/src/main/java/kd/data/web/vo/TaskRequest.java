package kd.data.web.vo;

import kd.data.core.model.SyncConfig;
import kd.data.service.entity.FieldMapping;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.List;
import java.util.Map;

/**
 * @author gaozw
 * @date 2025/7/25 17:51
 */
@Data
public class TaskRequest {

    @NotBlank(message = "任务id不能为空")
    private String taskId;

    @NotBlank(message = "任务名称不能为空")
    private String taskName;

    @NotBlank(message = "源任务类型不能为空")
    private String sourceType;

    @NotBlank(message = "目标任务类型不能为空")
    private String destinationType;

    @NotNull(message = "源任务配置不能为空")
    @Size(min = 1, message = "源任务配置不能为空")
    private Map<String, Object> sourceConfig;

    @NotNull(message = "目标任务配置不能为空")
    @Size(min = 1, message = "目标任务配置不能为空")
    private Map<String, Object> destinationConfig;

    private SyncConfig syncConfig;

    /**
     * 源名称
     */

    private String sourceFieldName;


    /**
     * 目标名称
     */

    private String targetFieldName;

    /**
     * 字段映射
     */

    private List<FieldMapping> fields;
}
