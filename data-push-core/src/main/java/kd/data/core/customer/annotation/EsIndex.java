package kd.data.core.customer.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Elasticsearch索引注解
 *
 * @author gaozw
 * @date 2025/7/25 14:39
 */
@SuppressWarnings("unused")
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface EsIndex {

 String value();
 String type() default "_doc";
}
