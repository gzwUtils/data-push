package kd.data.service.entity;
import lombok.Data;

/**
 * 字段映射
 *
 * @author gaozw
 * @date 2025/8/20 15:27
 */
@Data
public class FieldMapping {


    /**
     * Java 属性
     */
    private String javaField;

    /**
     * 源字段类型
     */

    private String sourceType;


    /**
     * 目标字段类型
     */

    private String targetType;


    /**
     * 源字段
     */
    private String sourceField;


    /**
     * 校验点
     */
    private Boolean checkpoint;

    /**
     * 目标字段
     */

    private String targetField;


    /**
     * 目标字段角色
     */

    private String role;
}
