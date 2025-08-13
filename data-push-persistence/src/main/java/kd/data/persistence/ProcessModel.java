package kd.data.persistence;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

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

    /**源配置*/
    private Map<String,Object> sourceConfig;

    /**目标配置*/
    private Map<String,Object> desConfig;




}
