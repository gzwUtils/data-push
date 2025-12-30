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
import java.lang.reflect.*;
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
public class JdbcDataAccessorAdapter<T> implements DataAccessor<T> {

    private final DataSource dataSource;
    private final JdbcTemplate jdbcTemplate;
    private final Class<T> entityType;
    private final DatabaseDialect dialect;
    private SyncConfig config;

    private String countQuery;
    private String baseCursorQuery;
    private String checkpointColumn;
    private final Map<String, String> columnMappings = new HashMap<>();

    // 缓存字段和setter方法
    private final Map<String, Field> fieldCache = new HashMap<>();
    private final Map<String, Method> setterCache = new HashMap<>();

    private final RowMapper<T> rowMapper;
    private final ClassLoader entityClassLoader;

    // 调试标志
    private final boolean debugMode ;

    public JdbcDataAccessorAdapter(DataSource dataSource, Class<T> entityType,
                                   DatabaseDialect dialect, SyncConfig config) {
        this.dataSource = dataSource;
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.entityType = entityType;
        this.dialect = dialect;
        this.config = config;
        this.entityClassLoader = entityType.getClassLoader();

        // 启用调试模式
        this.debugMode = log.isDebugEnabled();

        initEntityMapping();
        initFieldAndSetterCache();

        this.rowMapper = (rs, rowNum) -> mapRow(rs);
    }

    /**
     * 初始化字段和setter方法缓存
     */
    private void initFieldAndSetterCache() {
        for (Field field : entityType.getDeclaredFields()) {
            String fieldName = field.getName();
            fieldCache.put(fieldName, field);

            try {
                PropertyDescriptor pd = new PropertyDescriptor(fieldName, entityType);
                Method setter = pd.getWriteMethod();
                if (setter != null) {
                    setterCache.put(fieldName, setter);
                }
            } catch (Exception e) {
                log.debug("No setter found for field: {}", fieldName);
            }
        }
    }

    @Override
    public void init(SyncConfig config) {
        this.config = config;
        jdbcTemplate.setFetchSize(config.getBatchSize());
        jdbcTemplate.setMaxRows(0);

        buildQueries();
    }

    private void initEntityMapping() {
        Field[] fields = entityType.getDeclaredFields();
        for (Field field : fields) {
            ColumnMapping mapping = field.getAnnotation(ColumnMapping.class);
            String columnName = (mapping != null && !mapping.value().isEmpty()) ?
                    mapping.value() :
                    field.getName();
            columnMappings.put(field.getName(), columnName);

            if (mapping != null && mapping.isCheckpoint()) {
                if (checkpointColumn != null) {
                    throw new SyncException("Multiple checkpoint fields in: " + entityType.getName());
                }
                checkpointColumn = columnName;
                if (debugMode) {
                    log.debug("Checkpoint column detected: {} -> {} (field type: {})",
                            field.getName(), columnName, field.getType().getName());
                }
            }
        }

        if (checkpointColumn == null) {
            throw new SyncException("No checkpoint column defined for entity: " + entityType.getName());
        }
    }

    private void buildQueries() {
        String tableName = getTableName();

        countQuery = "SELECT COUNT(*) FROM " + tableName;
        baseCursorQuery = "SELECT * FROM " + tableName;

        if (debugMode) {
            log.debug("Built queries - Count: {}, Base: {}, Checkpoint: {}",
                    countQuery, baseCursorQuery, checkpointColumn);
        }
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

        Map<String, Object> params = new LinkedHashMap<>();
        params.put("p1", totalShards);
        params.put("p2", shardId);
        if (StringUtils.hasText(checkpoint)){
            params.put("p3", Long.valueOf(checkpoint.trim()));
        }

        if (debugMode) {
            log.debug("Opening cursor for shard {}/{}: {}", shardId, totalShards, sql);
        }

        return new StreamingJdbcCursor<>(dataSource, sql.toString(), params, rowMapper, dialect);
    }

    private final Map<Class<?>, Map<String, BiConsumer<Object, Object>>> classMapMap = new ConcurrentHashMap<>();

    private T mapRow(ResultSet rs) {
        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();

        try {
            Thread.currentThread().setContextClassLoader(entityClassLoader);

            Map<String, BiConsumer<Object, Object>> setters =
                    classMapMap.computeIfAbsent(entityType, this::buildSetterMap);
            ResultSetMetaData meta;
            T instance = createEntityInstance();

            meta = rs.getMetaData();
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                String col = meta.getColumnLabel(i);
                String colLower = col.toLowerCase();
                BiConsumer<Object, Object> setter = setters.get(colLower);

                if (setter != null) {
                    Object value = rs.getObject(i);
                    // 详细调试日志
                    if (debugMode) {
                        String columnType = meta.getColumnTypeName(i);
                        String javaType = value != null ? value.getClass().getName() : "null";
                        String fieldName = getFieldNameForColumn(colLower);
                        Field field = fieldCache.get(fieldName);
                        String targetType = field != null ? field.getType().getName() : "unknown";
                        log.debug("Mapping column: {} (DB: {}) value: {} (Java: {}) -> field: {} (Target: {})",
                                col, columnType, value, javaType, fieldName, targetType);
                    }

                    setter.accept(instance, value);
                }
            }
            return instance;
        } catch (Exception e) {
            throw new SyncException("Row mapping failed", e);
        } finally {
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }
    }

    /**
     * 根据列名获取字段名
     */
    private String getFieldNameForColumn(String columnName) {
        for (Map.Entry<String, String> entry : columnMappings.entrySet()) {
            if (entry.getValue().equalsIgnoreCase(columnName)) {
                return entry.getKey();
            }
        }
        return columnName;
    }

    /**
     * 创建实体实例
     */
    private T createEntityInstance() {
        try {
            return entityType.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new SyncException("Cannot create entity instance for: " + entityType.getName(), e);
        }
    }

    private Map<String, BiConsumer<Object, Object>> buildSetterMap(Class<?> clazz) {
        Map<String, BiConsumer<Object, Object>> map = new HashMap<>();

        for (Map.Entry<String, String> entry : columnMappings.entrySet()) {
            String fieldName = entry.getKey();
            String columnName = entry.getValue().toLowerCase();

            BiConsumer<Object, Object> setter = createReflectionSetter(fieldName);
            map.put(columnName, setter);
        }

        if (debugMode) {
            log.debug("Built setter map for {}: {} entries", clazz.getName(), map.size());
        }

        return map;
    }

    private BiConsumer<Object, Object> createReflectionSetter(String fieldName) {
        return (instance, value) -> {
            try {
                // 使用增强的ReflectionUtils（包含类型转换）
                ReflectionUtils.setFieldValue(instance, fieldName, value);
            } catch (Exception e) {
                // 如果反射设置失败，尝试使用setter方法
                try {
                    Method setter = setterCache.get(fieldName);
                    if (setter != null) {
                        // 尝试使用setter方法
                        Field field = fieldCache.get(fieldName);
                        if (field != null) {
                            // 先使用ReflectionUtils转换类型，再调用setter
                            Object convertedValue = ReflectionUtils.getFieldValue(instance, fieldName);
                            setter.invoke(instance, convertedValue);
                        } else {
                            // 直接调用setter，让ReflectionUtils在setFieldValue中处理类型转换
                            setter.invoke(instance, value);
                        }
                    } else {
                        throw new SyncException("No setter available for field: " + fieldName, e);
                    }
                } catch (Exception ex) {
                    throw new SyncException("Failed to set field value: " + fieldName, ex);
                }
            }
        };
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

            Object value = ReflectionUtils.getFieldValue(entity, checkpointFieldName);
            return value != null ? value.toString() : "";
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

    @Override
    public void close() {
        // 清理资源
        classMapMap.clear();
        fieldCache.clear();
        setterCache.clear();
        columnMappings.clear();
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
        private final boolean debugMode;

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
            this.debugMode = org.slf4j.LoggerFactory.getLogger(getClass()).isDebugEnabled();

            init();
        }

        private void init() {
            try {
                connection = DataSourceUtils.getConnection(dataSource);
                statement = dialect.prepareStatement(connection, sql);

                int index = 1;
                for (Map.Entry<String, Object> entry : params.entrySet()) {
                    statement.setObject(index++, entry.getValue());
                }

                dialect.configureStreaming(statement);
                resultSet = statement.executeQuery();
                hasNext = resultSet.next();

                if (debugMode) {
                    log.debug("JDBC cursor initialized for query: {}", sql);
                }

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