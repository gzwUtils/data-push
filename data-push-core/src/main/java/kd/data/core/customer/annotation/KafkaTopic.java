package kd.data.core.customer.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * kafka
 * @author gaozw
 * @date 2025/7/25 14:44
 */
@SuppressWarnings("unused")
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface KafkaTopic {
    String value();
}
