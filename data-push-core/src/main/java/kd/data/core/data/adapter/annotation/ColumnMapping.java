package kd.data.core.data.adapter.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 列映射注解
 * @author gaozw
 * @date 2025/7/24 16:06
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface ColumnMapping {

    String value() default "";
    boolean isCheckpoint() default false;
}
