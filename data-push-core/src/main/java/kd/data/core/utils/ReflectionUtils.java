package kd.data.core.utils;

import kd.data.core.exception.SyncException;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
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
     * 为对象字段设值，支持父类字段和类型转换
     */
    public static void setFieldValue(Object obj, String fieldName, Object value) {
        requireNonNull(obj, "obj");
        requireNonNull(fieldName, "fieldName");
        try {
            Field field = findField(obj.getClass(), fieldName);
            Object convertedValue = convertValueToFieldType(value, field.getType());
            field.set(obj, convertedValue);
        } catch (ReflectiveOperationException e) {
            throw new SyncException("Failed to set field [" + fieldName + "] on " + obj.getClass(), e);
        }
    }

    /**
     * 类型转换：将任意值转换为目标字段类型
     */
    private static Object convertValueToFieldType(Object value, Class<?> targetType) {
        if (value == null) {
            // 处理基础类型的null值
            if (targetType == int.class) return 0;
            if (targetType == long.class) return 0L;
            if (targetType == boolean.class) return false;
            if (targetType == double.class) return 0.0;
            if (targetType == float.class) return 0.0f;
            if (targetType == byte.class) return (byte) 0;
            if (targetType == short.class) return (short) 0;
            if (targetType == char.class) return '\0';
            return null;
        }

        // 如果已经是目标类型或兼容类型，直接返回
        if (targetType.isAssignableFrom(value.getClass())) {
            return value;
        }

        try {
            // 处理时间类型转换 - 必须先处理，因为Date类是其他类型的父类
            if (java.util.Date.class.isAssignableFrom(targetType)) {
                return convertToDate(value, targetType);
            } else if (targetType == LocalDateTime.class) {
                return convertToLocalDateTime(value);
            } else if (targetType == LocalDate.class) {
                return convertToLocalDate(value);
            } else if (targetType == java.sql.Date.class) {
                return convertToSqlDate(value);
            } else if (targetType == java.sql.Timestamp.class) {
                return convertToTimestamp(value);
            } else if (targetType == java.sql.Time.class) {
                return convertToSqlTime(value);
            }

            // 处理其他类型转换
            else if (targetType == Long.class || targetType == long.class) {
                return convertToLong(value);
            } else if (targetType == Integer.class || targetType == int.class) {
                return convertToInteger(value);
            } else if (targetType == String.class) {
                return value.toString();
            } else if (targetType == Boolean.class || targetType == boolean.class) {
                return convertToBoolean(value);
            } else if (targetType == Double.class || targetType == double.class) {
                return convertToDouble(value);
            } else if (targetType == Float.class || targetType == float.class) {
                return convertToFloat(value);
            } else if (targetType == BigDecimal.class) {
                return convertToBigDecimal(value);
            } else if (targetType == Byte.class || targetType == byte.class) {
                return convertToByte(value);
            } else if (targetType == Short.class || targetType == short.class) {
                return convertToShort(value);
            } else if (targetType == BigInteger.class) {
                return convertToBigInteger(value);
            } else if (value instanceof Number && Number.class.isAssignableFrom(targetType)) {
                return convertNumberToTarget((Number) value, targetType);
            }

            // 处理枚举类型
            if (targetType.isEnum()) {
                return Enum.valueOf((Class<Enum>) targetType, value.toString());
            }

            // 最后尝试使用String构造函数
            try {
                return targetType.getConstructor(String.class).newInstance(value.toString());
            } catch (NoSuchMethodException e) {
                // 没有String构造函数，返回原值
                return value;
            }
        } catch (Exception e) {
            throw new SyncException(String.format(
                    "Cannot convert value '%s' from %s to %s",
                    value, value.getClass().getName(), targetType.getName()), e);
        }
    }

    /**
     * 转换为Date类型
     */
    private static Object convertToDate(Object value, Class<?> targetType) {
        if (value == null) {
            return null;
        }

        try {
            // 首先处理Timestamp类型（MySQL的TIMESTAMP会返回这个）
            if (value instanceof java.sql.Timestamp) {
                Timestamp ts = (Timestamp) value;
                if (targetType == java.util.Date.class) {
                    return new java.util.Date(ts.getTime());
                } else if (targetType == java.sql.Date.class) {
                    return new java.sql.Date(ts.getTime());
                } else if (targetType == java.sql.Timestamp.class) {
                    return ts;
                }
            }
            // 处理java.sql.Date
            else if (value instanceof java.sql.Date) {
                java.sql.Date sqlDate = (java.sql.Date) value;
                if (targetType == java.util.Date.class) {
                    return new java.util.Date(sqlDate.getTime());
                } else if (targetType == java.sql.Date.class) {
                    return sqlDate;
                } else if (targetType == java.sql.Timestamp.class) {
                    return new Timestamp(sqlDate.getTime());
                }
            }
            // 处理java.util.Date
            else if (value instanceof java.util.Date) {
                java.util.Date date = (java.util.Date) value;
                if (targetType == java.util.Date.class) {
                    return date;
                } else if (targetType == java.sql.Date.class) {
                    return new java.sql.Date(date.getTime());
                } else if (targetType == java.sql.Timestamp.class) {
                    return new Timestamp(date.getTime());
                }
            }
            // 处理Long类型的时间戳
            else if (value instanceof Long) {
                long time = (Long) value;
                if (targetType == java.util.Date.class) {
                    return new java.util.Date(time);
                } else if (targetType == java.sql.Date.class) {
                    return new java.sql.Date(time);
                } else if (targetType == java.sql.Timestamp.class) {
                    return new Timestamp(time);
                }
            }
            // 处理String类型
            else if (value instanceof String) {
                String strValue = value.toString().trim();
                // 尝试解析时间戳
                try {
                    long timestamp = Long.parseLong(strValue);
                    if (targetType == java.util.Date.class) {
                        return new java.util.Date(timestamp);
                    } else if (targetType == java.sql.Date.class) {
                        return new java.sql.Date(timestamp);
                    } else if (targetType == java.sql.Timestamp.class) {
                        return new Timestamp(timestamp);
                    }
                } catch (NumberFormatException e) {
                    // 不是时间戳，尝试解析日期字符串
                    return parseDateString(strValue, targetType);
                }
            }

            throw new IllegalArgumentException("Cannot convert to " + targetType.getName() + ": " + value);
        } catch (Exception e) {
            throw new SyncException("Failed to convert to Date: " + value, e);
        }
    }

    /**
     * 解析日期字符串
     */
    private static Object parseDateString(String dateStr, Class<?> targetType) throws ParseException {
        // 尝试常见的日期格式
        SimpleDateFormat[] formats = {
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"),
                new SimpleDateFormat("yyyy-MM-dd"),
                new SimpleDateFormat("yyyy/MM/dd HH:mm:ss"),
                new SimpleDateFormat("yyyy/MM/dd"),
                new SimpleDateFormat("yyyyMMddHHmmss"),
                new SimpleDateFormat("yyyyMMdd")
        };

        java.util.Date date = null;
        for (SimpleDateFormat format : formats) {
            try {
                date = format.parse(dateStr);
                break;
            } catch (ParseException ignored) {
                // 继续尝试下一个格式
            }
        }

        if (date == null) {
            throw new IllegalArgumentException("Cannot parse date string: " + dateStr);
        }

        long time = date.getTime();
        if (targetType == java.util.Date.class) {
            return date;
        } else if (targetType == java.sql.Date.class) {
            return new java.sql.Date(time);
        } else if (targetType == java.sql.Timestamp.class) {
            return new Timestamp(time);
        } else {
            throw new IllegalArgumentException("Unsupported target type: " + targetType.getName());
        }
    }

    /**
     * 转换为LocalDateTime
     */
    private static LocalDateTime convertToLocalDateTime(Object value) {
        if (value == null) {
            return null;
        }

        try {
            if (value instanceof Timestamp) {
                return ((Timestamp) value).toLocalDateTime();
            } else if (value instanceof java.sql.Date) {
                return ((java.sql.Date) value).toLocalDate().atStartOfDay();
            } else if (value instanceof java.util.Date) {
                return LocalDateTime.ofInstant(
                        ((java.util.Date) value).toInstant(),
                        ZoneId.systemDefault()
                );
            } else if (value instanceof LocalDateTime) {
                return (LocalDateTime) value;
            } else if (value instanceof Long) {
                return LocalDateTime.ofInstant(
                        Instant.ofEpochMilli((Long) value),
                        ZoneId.systemDefault()
                );
            } else if (value instanceof String) {
                String str = value.toString().trim();
                // 尝试解析时间戳
                try {
                    long timestamp = Long.parseLong(str);
                    return LocalDateTime.ofInstant(
                            Instant.ofEpochMilli(timestamp),
                            ZoneId.systemDefault()
                    );
                } catch (NumberFormatException e) {
                    // 尝试解析日期字符串
                    try {
                        return LocalDateTime.parse(str);
                    } catch (Exception e1) {
                        try {
                            return LocalDateTime.parse(str, DateTimeFormatter.ISO_DATE_TIME);
                        } catch (Exception e2) {
                            // 尝试常见的日期格式
                            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                            Date date = sdf.parse(str);
                            return LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
                        }
                    }
                }
            }

            throw new IllegalArgumentException("Cannot convert to LocalDateTime: " + value);
        } catch (Exception e) {
            throw new SyncException("Failed to convert to LocalDateTime: " + value, e);
        }
    }

    /**
     * 转换为LocalDate
     */
    private static LocalDate convertToLocalDate(Object value) {
        if (value == null) {
            return null;
        }

        try {
            if (value instanceof java.sql.Date) {
                return ((java.sql.Date) value).toLocalDate();
            } else if (value instanceof java.util.Date) {
                return ((java.util.Date) value).toInstant()
                        .atZone(ZoneId.systemDefault())
                        .toLocalDate();
            } else if (value instanceof LocalDate) {
                return (LocalDate) value;
            } else if (value instanceof LocalDateTime) {
                return ((LocalDateTime) value).toLocalDate();
            } else if (value instanceof Long) {
                return Instant.ofEpochMilli((Long) value)
                        .atZone(ZoneId.systemDefault())
                        .toLocalDate();
            } else if (value instanceof String) {
                String str = value.toString().trim();
                // 尝试解析时间戳
                try {
                    long timestamp = Long.parseLong(str);
                    return Instant.ofEpochMilli(timestamp)
                            .atZone(ZoneId.systemDefault())
                            .toLocalDate();
                } catch (NumberFormatException e) {
                    // 尝试解析日期字符串
                    try {
                        return LocalDate.parse(str);
                    } catch (Exception e1) {
                        try {
                            return LocalDate.parse(str, DateTimeFormatter.ISO_DATE);
                        } catch (Exception e2) {
                            // 尝试常见的日期格式
                            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                            Date date = sdf.parse(str);
                            return date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
                        }
                    }
                }
            }

            throw new IllegalArgumentException("Cannot convert to LocalDate: " + value);
        } catch (Exception e) {
            throw new SyncException("Failed to convert to LocalDate: " + value, e);
        }
    }

    /**
     * 转换为Timestamp
     */
    private static Timestamp convertToTimestamp(Object value) {
        if (value == null) {
            return null;
        }

        try {
            if (value instanceof Timestamp) {
                return (Timestamp) value;
            } else if (value instanceof java.util.Date) {
                return new Timestamp(((java.util.Date) value).getTime());
            } else if (value instanceof java.sql.Date) {
                return new Timestamp(((java.sql.Date) value).getTime());
            } else if (value instanceof Long) {
                return new Timestamp((Long) value);
            } else if (value instanceof String) {
                String str = value.toString().trim();
                try {
                    long timestamp = Long.parseLong(str);
                    return new Timestamp(timestamp);
                } catch (NumberFormatException e) {
                    // 尝试解析日期字符串
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    Date date = sdf.parse(str);
                    return new Timestamp(date.getTime());
                }
            }

            throw new IllegalArgumentException("Cannot convert to Timestamp: " + value);
        } catch (Exception e) {
            throw new SyncException("Failed to convert to Timestamp: " + value, e);
        }
    }

    /**
     * 转换为SqlDate
     */
    private static java.sql.Date convertToSqlDate(Object value) {
        if (value == null) {
            return null;
        }

        try {
            if (value instanceof java.sql.Date) {
                return (java.sql.Date) value;
            } else if (value instanceof java.util.Date) {
                return new java.sql.Date(((java.util.Date) value).getTime());
            } else if (value instanceof Timestamp) {
                return new java.sql.Date(((Timestamp) value).getTime());
            } else if (value instanceof Long) {
                return new java.sql.Date((Long) value);
            } else if (value instanceof String) {
                String str = value.toString().trim();
                try {
                    long time = Long.parseLong(str);
                    return new java.sql.Date(time);
                } catch (NumberFormatException e) {
                    return java.sql.Date.valueOf(str);
                }
            }

            throw new IllegalArgumentException("Cannot convert to java.sql.Date: " + value);
        } catch (Exception e) {
            throw new SyncException("Failed to convert to java.sql.Date: " + value, e);
        }
    }

    /**
     * 转换为SqlTime
     */
    private static java.sql.Time convertToSqlTime(Object value) {
        if (value == null) {
            return null;
        }

        try {
            if (value instanceof java.sql.Time) {
                return (java.sql.Time) value;
            } else if (value instanceof java.util.Date) {
                return new java.sql.Time(((java.util.Date) value).getTime());
            } else if (value instanceof Timestamp) {
                return new java.sql.Time(((Timestamp) value).getTime());
            } else if (value instanceof Long) {
                return new java.sql.Time((Long) value);
            } else if (value instanceof String) {
                String str = value.toString().trim();
                try {
                    long time = Long.parseLong(str);
                    return new java.sql.Time(time);
                } catch (NumberFormatException e) {
                    return java.sql.Time.valueOf(str);
                }
            }

            throw new IllegalArgumentException("Cannot convert to Time: " + value);
        } catch (Exception e) {
            throw new SyncException("Failed to convert to Time: " + value, e);
        }
    }

    /**
     * 转换为Long
     */
    private static Long convertToLong(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        } else if (value instanceof BigInteger) {
            return ((BigInteger) value).longValue();
        } else if (value instanceof BigDecimal) {
            return ((BigDecimal) value).longValue();
        } else {
            return Long.parseLong(value.toString().trim());
        }
    }

    /**
     * 转换为Integer
     */
    private static Integer convertToInteger(Object value) {
        if (value instanceof Number) {
            return ((Number) value).intValue();
        } else if (value instanceof BigInteger) {
            return ((BigInteger) value).intValue();
        } else if (value instanceof BigDecimal) {
            return ((BigDecimal) value).intValue();
        } else {
            return Integer.parseInt(value.toString().trim());
        }
    }

    /**
     * 转换为Boolean
     */
    private static Boolean convertToBoolean(Object value) {
        if (value instanceof Boolean) {
            return (Boolean) value;
        } else if (value instanceof Number) {
            return ((Number) value).intValue() != 0;
        } else {
            String str = value.toString().toLowerCase().trim();
            return "1".equals(str) || "true".equals(str) || "yes".equals(str) || "on".equals(str);
        }
    }

    /**
     * 转换为Double
     */
    private static Double convertToDouble(Object value) {
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        } else {
            return Double.parseDouble(value.toString().trim());
        }
    }

    /**
     * 转换为Float
     */
    private static Float convertToFloat(Object value) {
        if (value instanceof Number) {
            return ((Number) value).floatValue();
        } else {
            return Float.parseFloat(value.toString().trim());
        }
    }

    /**
     * 转换为Byte
     */
    private static Byte convertToByte(Object value) {
        if (value instanceof Number) {
            return ((Number) value).byteValue();
        } else {
            return Byte.parseByte(value.toString().trim());
        }
    }

    /**
     * 转换为Short
     */
    private static Short convertToShort(Object value) {
        if (value instanceof Number) {
            return ((Number) value).shortValue();
        } else {
            return Short.parseShort(value.toString().trim());
        }
    }

    /**
     * 转换为BigDecimal
     */
    private static BigDecimal convertToBigDecimal(Object value) {
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        } else if (value instanceof BigInteger) {
            return new BigDecimal((BigInteger) value);
        } else if (value instanceof Number) {
            return new BigDecimal(value.toString());
        } else {
            return new BigDecimal(value.toString().trim());
        }
    }

    /**
     * 转换为BigInteger
     */
    private static BigInteger convertToBigInteger(Object value) {
        if (value instanceof BigInteger) {
            return (BigInteger) value;
        } else if (value instanceof BigDecimal) {
            return ((BigDecimal) value).toBigInteger();
        } else if (value instanceof Number) {
            return BigInteger.valueOf(((Number) value).longValue());
        } else {
            return new BigInteger(value.toString().trim());
        }
    }

    /**
     * 通用Number类型转换
     */
    private static Object convertNumberToTarget(Number number, Class<?> targetType) {
        if (targetType == Integer.class || targetType == int.class) {
            return number.intValue();
        } else if (targetType == Long.class || targetType == long.class) {
            return number.longValue();
        } else if (targetType == Double.class || targetType == double.class) {
            return number.doubleValue();
        } else if (targetType == Float.class || targetType == float.class) {
            return number.floatValue();
        } else if (targetType == Short.class || targetType == short.class) {
            return number.shortValue();
        } else if (targetType == Byte.class || targetType == byte.class) {
            return number.byteValue();
        } else if (targetType == BigDecimal.class) {
            return new BigDecimal(number.toString());
        } else if (targetType == BigInteger.class) {
            return BigInteger.valueOf(number.longValue());
        } else {
            return number;
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