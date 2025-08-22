package kd.data.core.customer.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 通用字段注解
 *
 * @author gaozw
 * @date 2025/7/25 14:45
 */
@SuppressWarnings("unused")
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface ConsumerField {

    String value() default "";
    FieldRole role() default FieldRole.DATA;

    enum FieldRole {
        ID,         // 主键/文档ID
        DATA,       // 普通数据字段
        TIMESTAMP,  // 时间戳字段
        VERSION,     // 版本字段

        KEY        // KAFKA key
    }
}
