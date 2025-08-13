package kd.data.core.send.adapter.annotation;



import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 *
 * 表映射注解
 * @author gaozw
 * @date 2025/7/24 16:04
 */


@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface TableMapping {

    String value() default "";
}
