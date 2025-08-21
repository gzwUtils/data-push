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

import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

/**
 * 字段解析生成 - 优化版本，添加类缓存
 *
 * @author gaozw
 * @date 2025/8/20 16:05
 */
@SuppressWarnings({"all"})
public class EntityGenerator {

    private EntityGenerator(){}

    // 缓存生成的类
    private static final Map<String, Class<?>> GENERATED_CLASSES = new ConcurrentHashMap<>();

    // 缓存类加载器
    private static final ClassLoader BASE_CLASS_LOADER = EntityGenerator.class.getClassLoader();

    // 生成 Source 实体
    public static Class<?> buildSource(String className, SyncTaskConfig req) {
        // 检查缓存
        if (GENERATED_CLASSES.containsKey(className)) {
            return GENERATED_CLASSES.get(className);
        }

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

        // 缓存生成的类
        GENERATED_CLASSES.put(className, generatedClass);
        return generatedClass;
    }

    // 生成 Target 实体
    public static Class<?> buildTarget(String className, SyncTaskConfig req) {
        // 检查缓存
        if (GENERATED_CLASSES.containsKey(className)) {
            return GENERATED_CLASSES.get(className);
        }

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
                break;
            case KAFKA:
                builder = builder.annotateType(AnnotationDescription.Builder.ofType(KafkaTopic.class)
                        .define("value", targetName).build());
                break;
            default:
                throw new IllegalArgumentException("Unsupported target type: " + targetType);
        }

        for (FieldMapping f : req.getFields()) {
            builder = builder.defineField(f.getJavaField(), toClass(f.getTargetType()), Modifier.PRIVATE)
                    .annotateField(AnnotationDescription.Builder.ofType(ConsumerField.class)
                            .define("value", f.getTargetField())
                            .define("role", getFieldRoleFromString(f.getRole()))
                            .build());

            builder = addGetterSetter(builder, f.getJavaField(), toClass(f.getTargetType()));
        }

        Class<?> generatedClass = builder.make()
                .load(BASE_CLASS_LOADER, ClassLoadingStrategy.Default.WRAPPER)
                .getLoaded();

        // 缓存生成的类
        GENERATED_CLASSES.put(className, generatedClass);
        return generatedClass;
    }

    // 获取类加载器
    public static ClassLoader getClassLoader() {
        return BASE_CLASS_LOADER;
    }

    // 获取缓存的类
    public static Class<?> getCachedClass(String className) {
        return GENERATED_CLASSES.get(className);
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
}