package kd.data.core.customer.meta;

import kd.data.core.customer.annotation.*;
import lombok.Getter;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * 消费者目标元数据
 * 仅包含消费者相关的元信息
 * @author gaozw
 * @date 2025/7/25 15:53
 */
@Getter
public class ConsumerMetadata {


    private final Class<?> entityType;
    private String targetType;
    private String tableName;
    private String indexName;
    private String topicName;
    private final List<ConsumerFieldModel> fields = new ArrayList<>();

    public ConsumerMetadata(Class<?> entityType) {
        this.entityType = entityType;
        parseAnnotations();
    }

    private void parseAnnotations() {
        // 解析目标类型
        if (entityType.isAnnotationPresent(ConsumerTarget.class)) {
            ConsumerTarget annotation = entityType.getAnnotation(ConsumerTarget.class);
            this.targetType = annotation.value().name();
        }

        // 解析特定目标注解

        if (entityType.isAnnotationPresent(CustomerTableName.class)) {
            CustomerTableName annotation = entityType.getAnnotation(CustomerTableName.class);
            this.tableName = annotation.value();
        }


        if (entityType.isAnnotationPresent(EsIndex.class)) {
            EsIndex annotation = entityType.getAnnotation(EsIndex.class);
            this.indexName = annotation.value();
        }

        if (entityType.isAnnotationPresent(KafkaTopic.class)) {
            KafkaTopic annotation = entityType.getAnnotation(KafkaTopic.class);
            this.topicName = annotation.value();
        }

        // 解析字段注解
        for (Field field : entityType.getDeclaredFields()) {
            if (field.isAnnotationPresent(ConsumerField.class)) {

                ConsumerField annotation = field.getAnnotation(ConsumerField.class);
                fields.add(new ConsumerFieldModel(
                        field.getName(),
                        annotation.value(),
                        annotation.role().name()
                ));
            }
        }
    }


    /**
     * 消费者字段模型
     */

    @Getter
    public static class ConsumerFieldModel {
        private final String fieldName;

        private final String targetName;

        private final String role;

        public ConsumerFieldModel(String fieldName, String targetName, String role) {
            this.fieldName = fieldName;
            this.targetName = targetName;
            this.role = role;
        }
    }
}
