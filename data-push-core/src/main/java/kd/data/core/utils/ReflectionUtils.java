package kd.data.core.utils;

import kd.data.core.exception.SyncException;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author gaozw
 * @date 2025/7/24 16:02
 */
@SuppressWarnings("all")
public final class ReflectionUtils {


    private static final Map<Class<?>, Map<String, Field>> FIELD_CACHE = new ConcurrentHashMap<>();

    private ReflectionUtils() {
        throw new AssertionError("No ReflectionUtils instances for you!");
    }

    /**
     * 为对象字段设值，支持父类字段
     */
    public static void setFieldValue(Object obj, String fieldName, Object value) {
        requireNonNull(obj, "obj");
        requireNonNull(fieldName, "fieldName");
        try {
            Field field = findField(obj.getClass(), fieldName);
            field.set(obj, value);
        } catch (ReflectiveOperationException e) {
            throw new SyncException("Failed to set field [" + fieldName + "] on " + obj.getClass(), e);
        }
    }

    /**
     * 读取对象字段值，支持父类字段
     */
    public static Object getFieldValue(Object obj, String fieldName) {
        requireNonNull(obj, "obj");
        requireNonNull(fieldName, "fieldName");
        try {
            Field field = findField(obj.getClass(), fieldName);
            return field.get(obj);
        } catch (ReflectiveOperationException e) {
            throw new SyncException("Failed to get field [" + fieldName + "] from " + obj.getClass(), e);
        }
    }

    /**
     * 查找字段（含父类），结果缓存
     */
    private static Field findField(Class<?> clazz, String fieldName) throws NoSuchFieldException {
        Map<String, Field> fieldMap = FIELD_CACHE.computeIfAbsent(clazz, k -> new ConcurrentHashMap<>());
        Field field = fieldMap.get(fieldName);
        if (field == null) {
            field = searchFieldHierarchy(clazz, fieldName);
            field.setAccessible(true);
            fieldMap.put(fieldName, field);
        }
        return field;
    }

    private static Field searchFieldHierarchy(Class<?> clazz, String fieldName) throws NoSuchFieldException {
        Class<?> current = clazz;
        while (current != null && current != Object.class) {
            try {
                return current.getDeclaredField(fieldName);
            } catch (NoSuchFieldException ignored) {
                current = current.getSuperclass();
            }
        }
        throw new NoSuchFieldException("Field '" + fieldName + "' not found on " + clazz);
    }

    private static void requireNonNull(Object o, String name) {
        if (o == null) {
            throw new SyncException(name + " must not be null");
        }
    }
}
