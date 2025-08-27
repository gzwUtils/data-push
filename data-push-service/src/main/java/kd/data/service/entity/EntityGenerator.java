package kd.data.service.entity;

import kd.data.core.customer.annotation.*;
import kd.data.core.customer.target.targetenums.TargetEnums;
import kd.data.core.send.adapter.annotation.ColumnMapping;
import kd.data.core.send.adapter.annotation.TableMapping;
import kd.data.service.model.SyncTaskConfig;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.annotation.AnnotationDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.FieldAccessor;
import org.springframework.data.elasticsearch.annotations.DateFormat;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 字段解析生成 - 优化版本，添加类缓存
 *
 * @author gaozw
 * @date 2025/8/20 16:05
 */
@SuppressWarnings({"all"})
public class EntityGenerator {

    private EntityGenerator(){}



    // 缓存类加载器
    private static final ClassLoader BASE_CLASS_LOADER = EntityGenerator.class.getClassLoader();

    // 生成 Source 实体
    public static Class<?> buildSource(String className, SyncTaskConfig req) {

        DynamicType.Builder<Object> builder = new ByteBuddy()
                .subclass(Object.class)
                .name(className)
                .annotateType(AnnotationDescription.Builder.ofType(TableMapping.class)
                        .define("value", req.getSourceFieldName()).build());

        for (FieldMapping f : req.getFields()) {
            builder = builder.defineField(f.getJavaField(), toClass(f.getSourceType()), Modifier.PRIVATE)
                    .annotateField(AnnotationDescription.Builder.ofType(ColumnMapping.class)
                            .define("value", f.getSourceField())
                            .define("isCheckpoint", f.getCheckpoint())
                            .build());

            builder  =   addGetterSetter(builder,f.getJavaField(),toClass(f.getSourceType()));
        }

        Class<?> generatedClass = builder.make()
                .load(BASE_CLASS_LOADER, ClassLoadingStrategy.Default.WRAPPER)
                .getLoaded();

        return generatedClass;
    }

    // 生成 Target 实体
    public static Class<?> buildTarget(String className, SyncTaskConfig req) {

        // 获取目标类型
        TargetEnums targetType = getTargetTypeFromString(req.getDestinationType());

        DynamicType.Builder<Object> builder = new ByteBuddy()
                .subclass(Object.class)
                .name(className)
                // 添加 ConsumerTarget 注解
                .annotateType(AnnotationDescription.Builder.ofType(ConsumerTarget.class)
                        .define("value", targetType).build());

        // 根据目标类型添加相应的表/索引/主题注解
        String targetName = req.getTargetFieldName();
        switch (targetType) {
            case JDBC:
                builder = builder.annotateType(AnnotationDescription.Builder.ofType(CustomerTableName.class)
                        .define("value", targetName).build());
                break;
            case ELASTICSEARCH:
                builder = builder.annotateType(AnnotationDescription.Builder.ofType(EsIndex.class)
                        .define("value", targetName).build());

                builder = builder.annotateType(AnnotationDescription.Builder.ofType(
                                org.springframework.data.elasticsearch.annotations.Document.class)
                        .define("indexName", targetName)
                        .define("createIndex", false)
                        .build());
                break;
            case KAFKA:
                builder = builder.annotateType(AnnotationDescription.Builder.ofType(KafkaTopic.class)
                        .define("value", targetName).build());
                break;
            default:
                throw new IllegalArgumentException("Unsupported target type: " + targetType);
        }

        for (FieldMapping f : req.getFields()) {
            // 收集所有需要添加的注解
            List<AnnotationDescription> annotations = new ArrayList<>();

            if (targetType == TargetEnums.ELASTICSEARCH) {
                annotations.add(createElasticsearchFieldAnnotation(f, toClass(f.getTargetType())));
            } else {
                // 添加 ConsumerField 注解
                annotations.add(AnnotationDescription.Builder.ofType(ConsumerField.class)
                        .define("value", f.getTargetField())
                        .define("role", getFieldRoleFromString(f.getRole()))
                        .build());
            }
            // 定义字段并添加所有注解
            DynamicType.Builder<Object> tempBuilder = builder;
            for (AnnotationDescription annotation : annotations) {
                tempBuilder = tempBuilder.defineField(f.getJavaField(), toClass(f.getTargetType()), Modifier.PRIVATE)
                        .annotateField(annotation);
            }
            builder = tempBuilder;

            builder = addGetterSetter(builder, f.getJavaField(), toClass(f.getTargetType()));
        }

        Class<?> generatedClass = builder.make()
                .load(BASE_CLASS_LOADER, ClassLoadingStrategy.Default.WRAPPER)
                .getLoaded();

        return generatedClass;
    }

    // 获取类加载器
    public static ClassLoader getClassLoader() {
        return BASE_CLASS_LOADER;
    }

    private static Class<?> toClass(String type) {
        switch (type) {
            case "String":
                return String.class;
            case "Long":
                return Long.class;
            case "Integer":
                return Integer.class;
            case "Double":
                return Double.class;
            case "BigDecimal":
                return BigDecimal.class;
            case "LocalDate":
                return LocalDate.class;
            case "LocalDateTime":
                return LocalDateTime.class;
            default:
                throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    public static ConsumerField.FieldRole getFieldRoleFromString(String roleStr) {
        if (roleStr == null || roleStr.trim().isEmpty()) {
            return ConsumerField.FieldRole.DATA; // 默认值
        }

        try {
            // 转换为大写并去除空格，实现不区分大小写的匹配
            return ConsumerField.FieldRole.valueOf(roleStr.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            // 提供更友好的错误信息
            throw new IllegalArgumentException("Invalid FieldRole: '" + roleStr +
                    "'. Valid values are: " + Arrays.toString(ConsumerField.FieldRole.values()));
        }
    }

    // 在 EntityGenerator 中添加生成 getter/setter 的方法
    private static DynamicType.Builder<Object> addGetterSetter(
            DynamicType.Builder<Object> builder, String fieldName, Class<?> fieldType) {

        String capitalized = fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);

        // 生成 getter 方法
        builder = builder.defineMethod("get" + capitalized, fieldType, Modifier.PUBLIC)
                .intercept(FieldAccessor.ofField(fieldName));

        // 生成 setter 方法
        builder = builder.defineMethod("set" + capitalized, void.class, Modifier.PUBLIC)
                .withParameter(fieldType, "value")
                .intercept(FieldAccessor.ofField(fieldName));

        return builder;
    }

    // 新增方法：将字符串转换为 TargetType 枚举
    public static TargetEnums getTargetTypeFromString(String targetTypeStr) {
        if (targetTypeStr == null || targetTypeStr.trim().isEmpty()) {
            throw new IllegalArgumentException("Target type cannot be null or empty");
        }

        try {
            return TargetEnums.valueOf(targetTypeStr.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid TargetType: '" + targetTypeStr +
                    "'. Valid values are: " + Arrays.toString(TargetEnums.values()));
        }
    }

    // 为 Elasticsearch 字段添加注解
    private static AnnotationDescription createElasticsearchFieldAnnotation(
            FieldMapping fieldMapping, Class<?> fieldClass) {

        // 创建 @Field 注解构建器
        AnnotationDescription.Builder fieldAnnotationBuilder = AnnotationDescription.Builder
                .ofType(Field.class)
                .define("name", fieldMapping.getTargetField());

        // 根据字段类型设置不同的 FieldType
        if (fieldClass == String.class) {
            fieldAnnotationBuilder = fieldAnnotationBuilder.define("type", FieldType.Text);
            // 如果是 ID 字段，不需要分词
            if (fieldMapping.getRole() != null &&
                    fieldMapping.getRole().equalsIgnoreCase("id")) {
                fieldAnnotationBuilder = fieldAnnotationBuilder.define("index", false);
            }
        } else if (fieldClass == Long.class || fieldClass == BigInteger.class) {
            fieldAnnotationBuilder = fieldAnnotationBuilder.define("type", FieldType.Long);
        } else if (fieldClass == Integer.class) {
            fieldAnnotationBuilder = fieldAnnotationBuilder.define("type", FieldType.Integer);
        } else if (fieldClass == Double.class) {
            fieldAnnotationBuilder = fieldAnnotationBuilder.define("type", FieldType.Double);
        } else if (fieldClass == BigDecimal.class) {
            fieldAnnotationBuilder = fieldAnnotationBuilder.define("type", FieldType.Double);
        } else if (fieldClass == LocalDate.class || fieldClass == LocalDateTime.class) {
            fieldAnnotationBuilder = fieldAnnotationBuilder
                    .define("type", FieldType.Date)
                    .defineEnumerationArray("format",
                            DateFormat.class,DateFormat.date_hour_minute_second_millis);
        } else {
            // 默认类型
            fieldAnnotationBuilder = fieldAnnotationBuilder.define("type", FieldType.Auto);
        }

        return fieldAnnotationBuilder.build();
    }
}