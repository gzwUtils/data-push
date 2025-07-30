package kd.data.core.customer.target.targetenums;

import lombok.Getter;
import org.springframework.util.StringUtils;

/**
 * 消费端枚举类型
 *
 * @author gaozw
 * @date 2025/7/28 16:09
 */
@Getter
public enum TargetEnums {

    JDBC,ELASTICSEARCH,KAFKA,RABBITMQ;



    public static TargetEnums getTargetEnum(String targetType) {

        if(!StringUtils.hasText(targetType)){
            return null;
        }
        for (TargetEnums targetEnum : TargetEnums.values()) {
            if (targetEnum.name().equalsIgnoreCase(targetType)) {
                return targetEnum;
            }
        }
        return null;
    }
}
