package kd.data.core.customer.annotation;

import kd.data.core.customer.target.targetenums.TargetEnums;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 通用目标注解
 * 支持多种目标类型：JDBC、ES、MQ
 *
 * @author gaozw
 * @date 2025/7/25 14:38
 */
@SuppressWarnings("unused")
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface ConsumerTarget {

    TargetEnums value() default TargetEnums.JDBC;
}
