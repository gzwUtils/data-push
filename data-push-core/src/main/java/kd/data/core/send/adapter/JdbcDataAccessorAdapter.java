package kd.data.core.send.adapter;

import kd.data.core.send.adapter.annotation.ColumnMapping;
import kd.data.core.send.adapter.annotation.TableMapping;
import kd.data.core.send.adapter.dialect.DatabaseDialect;
import kd.data.core.exception.SyncException;
import kd.data.core.model.SyncConfig;
import kd.data.core.send.DataAccessor;
import kd.data.core.utils.ReflectionUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.*;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.util.StringUtils;
import javax.sql.DataSource;
import java.beans.PropertyDescriptor;
import java.lang.invoke.*;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/**
 * 优化后的JDBC通用数据访问器
 * 支持流式游标、连接池管理、数据库方言适配
 */
@SuppressWarnings("unused")
@Slf4j
public  class JdbcDataAccessorAdapter<T> implements DataAccessor<T> {

    private final DataSource dataSource;
    private final JdbcTemplate jdbcTemplate;
    private final Class<T> entityType;
    private final DatabaseDialect dialect;
    private SyncConfig config;

    private String countQuery;
    private String baseCursorQuery;
    private String checkpointColumn;
    private final Map<String, String> columnMappings = new HashMap<>();

    private final RowMapper<T> rowMapper;
    private final ClassLoader entityClassLoader;

    public JdbcDataAccessorAdapter(DataSource dataSource, Class<T> entityType,
                                   DatabaseDialect dialect, SyncConfig config) {
        this.dataSource = dataSource;
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.entityType = entityType;
        this.dialect = dialect;
        this.config = config;
        this.entityClassLoader = entityType.getClassLoader();
        initEntityMapping();

        // 放到这里，entityType 已确定赋值
        this.rowMapper = (rs, rowNum) -> mapRow(rs);
    }

    @Override
    public void init(SyncConfig config) {
        this.config = config;
        jdbcTemplate.setFetchSize(config.getBatchSize());
        jdbcTemplate.setMaxRows(0); // 无限制

        // 构建查询语句
        buildQueries();
    }

    private void initEntityMapping() {
        // 使用反射获取实体类字段与数据库列的映射
        Field[] fields = entityType.getDeclaredFields();
        for (Field field : fields) {
            ColumnMapping mapping = field.getAnnotation(ColumnMapping.class);
            String columnName = (mapping != null && !mapping.value().isEmpty()) ?
                    mapping.value() :
                    field.getName();
            columnMappings.put(field.getName(), columnName);

            // 检查是否为检查点字段
            if (mapping != null && mapping.isCheckpoint()) {
                if (checkpointColumn != null) {
                    throw new SyncException("Multiple checkpoint fields in: " + entityType.getName());
                }
                checkpointColumn = columnName;
            }
        }

        if (checkpointColumn == null) {
            throw new SyncException("No checkpoint column defined for entity: " + entityType.getName());
        }
    }

    private void buildQueries() {
        String tableName = getTableName();

        // 构建计数查询
        countQuery = "SELECT COUNT(*) FROM " + tableName;

        // 构建基础游标查询
        baseCursorQuery = "SELECT * FROM " + tableName;
    }

    private String getTableName() {
        TableMapping tableMapping = entityType.getAnnotation(TableMapping.class);
        if (tableMapping != null && !tableMapping.value().isEmpty()) {
            return tableMapping.value();
        }
        return entityType.getSimpleName();
    }

    @Override
    public long countRecords() {
        try {
            Long count = jdbcTemplate.queryForObject(countQuery, Long.class);
            return count != null ? count : 0L;
        } catch (Exception e) {
            throw new SyncException("Failed to count records", e);
        }
    }

    @Override
    public Cursor<T> openCursor(int shardId, int totalShards, String checkpoint) {
        StringBuilder sql = new StringBuilder(baseCursorQuery)
                .append(" WHERE MOD(").append(checkpointColumn).append(", ?) = ?");

        if (StringUtils.hasText(checkpoint)) {
            sql.append(" AND ").append(checkpointColumn).append(" > ?");
        }

        sql.append(" ORDER BY ").append(checkpointColumn);

        // 如果 StreamingJdbcCursor 只接受 Map，就把顺序值再塞进 Map
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("p1", totalShards);
        params.put("p2", shardId);
        if (StringUtils.hasText(checkpoint)){
            params.put("p3", Long.valueOf(checkpoint.trim()));
        }
        return new StreamingJdbcCursor<>(dataSource, sql.toString(), params, rowMapper, dialect);
    }


    /* ---------- 4. 缓存 <字段名 -> Setter> ---------- */
    private  final Map<Class<?>, Map<String, BiConsumer<Object, Object>>> classMapMap = new ConcurrentHashMap<>();

    private T mapRow(ResultSet rs) {
        // 保存当前线程的类加载器
        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();

        try {
            // 切换到实体类的类加载器
            Thread.currentThread().setContextClassLoader(entityClassLoader);

            Map<String, BiConsumer<Object, Object>> setters =
                    classMapMap.computeIfAbsent(entityType, this::buildSetterMap);

            ResultSetMetaData meta;
            T instance = entityType.newInstance();

            meta = rs.getMetaData();
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                String col = meta.getColumnLabel(i);   // 优先 label，兼容 AS
                BiConsumer<Object, Object> setter = setters.get(col.toLowerCase());
                if (setter != null) {
                    setter.accept(instance, rs.getObject(i));
                }
            }
            return instance;
        } catch (Exception e) {
            throw new SyncException("Row mapping failed", e);
        } finally {
            // 恢复原来的类加载器
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }
    }

    private Map<String, BiConsumer<Object, Object>> buildSetterMap(Class<?> clazz) {
        Map<String, BiConsumer<Object, Object>> map = new HashMap<>();

        for (Field field : clazz.getDeclaredFields()) {
            String columnName = Optional.ofNullable(columnMappings.get(field.getName()))
                    .orElse(field.getName())
                    .toLowerCase();

            // 使用传统的反射方式创建setter
            BiConsumer<Object, Object> setter = createReflectionSetter(field);
            map.put(columnName, setter);
        }
        return map;
    }

    private BiConsumer<Object, Object> createReflectionSetter(Field field) {
        return (instance, value) -> {
            try {
                ReflectionUtils.setFieldValue(instance,field.getName(),value);
            } catch (Exception e) {
                try {
                    PropertyDescriptor pd = new PropertyDescriptor(field.getName(), field.getDeclaringClass());
                    Method setter = pd.getWriteMethod();
                    if (setter != null) {
                        setter.invoke(instance, value);
                    } else {
                        throw new SyncException("No setter available for field: " + field.getName(), e);
                    }
                } catch (Exception ex) {
                    throw new SyncException("Failed to set field value: " + field.getName(), ex);
                }
            }
        };
    }

    @SuppressWarnings("unchecked")
    private void setter(Map<String, BiConsumer<Object, Object>> map, Field field, String columnName, MethodHandle handle) throws Throwable {
        MethodType invokedType =
                MethodType.methodType(void.class, Object.class, field.getType());
        CallSite site = LambdaMetafactory.metafactory(
                MethodHandles.lookup(),
                "accept",
                MethodType.methodType(BiConsumer.class),
                handle.type().erase(),
                handle,
                handle.type());
        BiConsumer<Object, Object> setter =
                (BiConsumer<Object, Object>) site.getTarget().invokeExact();
        map.put(columnName, setter);
    }

    /* 6. 创建实例：缓存无参构造器，避免每次都 newInstance */
    private static <T> T newInstance(Class<T> clazz) {
        try {
            return clazz.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new SyncException("Cannot create entity instance", e);
        }
    }

    @Override
    public String getRecordId(T entity) {
        try {
            // 通过检查点列名反查字段名
            String checkpointFieldName = columnMappings.entrySet().stream()
                    .filter(entry -> entry.getValue().equalsIgnoreCase(checkpointColumn))
                    .map(Map.Entry::getKey)
                    .findFirst()
                    .orElseThrow(() -> new SyncException("Checkpoint field not found for column: " + checkpointColumn));

            return ReflectionUtils.getFieldValue(entity, checkpointFieldName).toString();
        } catch (Exception e) {
            throw new SyncException("Failed to get record ID", e);
        }
    }

    @Override
    public String getFilterKey(T entity) {
        // 默认实现，子类可覆盖
        return "";
    }

    @Override
    @SuppressWarnings("all")
    public String getMaxCheckpointInShard(int shardId, int totalShards) {

        // 1. 构造子查询
        String subQuery = baseCursorQuery
                + " WHERE MOD(" + checkpointColumn + ", ?) = ?";

        String sql = "SELECT MAX(" + checkpointColumn + ") FROM (" + subQuery + ") AS t";

        // 2. 执行
        String maxCheckpoint = jdbcTemplate.queryForObject(
                sql,
                String.class,
                totalShards,
                shardId);

        // 3. 兜底：null 返回空串或自定义初始值
        return maxCheckpoint == null ? "" : maxCheckpoint;
    }


    /**
     * 流式JDBC游标实现
     */
    static class StreamingJdbcCursor<T> implements DataAccessor.Cursor<T> {
        private final DataSource dataSource;
        private final String sql;
        private final Map<String, Object> params;
        private final RowMapper<T> rowMapper;
        private final DatabaseDialect dialect;

        private Connection connection;
        private PreparedStatement statement;
        private ResultSet resultSet;
        private boolean hasNext;
        private int rowCount = 0;

        public StreamingJdbcCursor(DataSource dataSource,
                                   String sql,
                                   Map<String, Object> params,
                                   RowMapper<T> rowMapper,
                                   DatabaseDialect dialect) {
            this.dataSource = dataSource;
            this.sql = sql;
            this.params = params;
            this.rowMapper = rowMapper;
            this.dialect = dialect;

            init();
        }

        private void init() {
            try {
                // 获取连接（使用Spring的连接工具）
                connection = DataSourceUtils.getConnection(dataSource);

                // 创建预处理语句
                statement = dialect.prepareStatement(connection, sql);

                // 设置参数
                int index = 1;
                for (Map.Entry<String, Object> entry : params.entrySet()) {
                    statement.setObject(index++, entry.getValue());
                }

                // 配置流式读取
                dialect.configureStreaming(statement);

                // 执行查询
                resultSet = statement.executeQuery();
                hasNext = resultSet.next();

            } catch (SQLException e) {
                closeResources();
                throw new SyncException("Failed to open JDBC cursor", e);
            }
        }

        @Override
        public boolean hasNext() {
            return hasNext;
        }

        @Override
        public T next() {
            if (!hasNext) {
                throw new IllegalStateException("No more elements");
            }

            try {
                T result = rowMapper.mapRow(resultSet, rowCount++);
                hasNext = resultSet.next();
                return result;
            } catch (SQLException e) {
                throw new SyncException("Error reading next row", e);
            }
        }

        @Override
        public void close() {
            closeResources();
        }

        private void closeResources() {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
            } catch (SQLException e) {
                // 记录日志
            } finally {
                resultSet = null;

                try {
                    if (statement != null) {
                        statement.close();
                    }
                } catch (SQLException e) {
                    // 记录日志
                } finally {
                    statement = null;

                    if (connection != null) {
                        DataSourceUtils.releaseConnection(connection, dataSource);
                        connection = null;
                    }
                }
            }
        }
    }
}
