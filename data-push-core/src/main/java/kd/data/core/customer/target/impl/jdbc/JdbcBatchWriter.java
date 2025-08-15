package kd.data.core.customer.target.impl.jdbc;

import kd.data.core.customer.meta.ConsumerMetadata;
import kd.data.core.customer.target.TargetConnector;
import kd.data.core.customer.target.TargetWriter;
import kd.data.core.exception.SyncException;
import kd.data.core.utils.ReflectionUtils;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author gaozw
 * @date 2025/7/24 16:43
 */
@SuppressWarnings({"unused"})
public class JdbcBatchWriter<T> implements TargetWriter<T> {

    // 使用原子引用确保安全发布
    private final AtomicReference<JdbcTemplate> jdbcTemplateRef = new AtomicReference<>();
    private final AtomicReference<TransactionTemplate> transactionTemplateRef = new AtomicReference<>();

    // 细粒度锁控制初始化
    private final ReentrantLock initLock = new ReentrantLock();

    // 初始化状态标志
    private volatile boolean initialized = false;

    @Override
    public void write(List<T> batch, TargetConnector connector, ConsumerMetadata metadata) {
        if (!(connector instanceof JdbcTargetConnector)) {
            throw new IllegalArgumentException("Invalid connector for JDBC writer");
        }

        // 线程安全的延迟初始化
        initializeIfNeeded((JdbcTargetConnector) connector);

        // 生成插入SQL
        String tableName = getTableName(metadata);
        String sql = generateInsertSql(tableName, metadata);

        // 执行批量写入
        executeBatch(batch, sql, metadata.getFields());
    }

    /**
     * 线程安全的初始化方法
     */
    private void initializeIfNeeded(JdbcTargetConnector connector) {

        if (initialized){
            return;
        }
        initLock.lock();
        try {
            // 双重检查锁定
            if (!initialized) {
                DataSource dataSource = connector.getDataSource();
                if (dataSource == null) {
                    throw new IllegalStateException("DataSource cannot be null");
                }

                // 创建新实例
                JdbcTemplate newJdbcTemplate = new JdbcTemplate(dataSource);
                DataSourceTransactionManager txManager =
                        new DataSourceTransactionManager(dataSource);

                TransactionTemplate newTxTemplate = new TransactionTemplate(txManager);
                newTxTemplate.setPropagationBehavior(
                        TransactionDefinition.PROPAGATION_REQUIRED);

                // 安全发布到其他线程
                jdbcTemplateRef.set(newJdbcTemplate);
                transactionTemplateRef.set(newTxTemplate);

                // 内存屏障确保可见性
                initialized = true;
            }
        } finally {
            initLock.unlock();
        }
    }

    private String getTableName(ConsumerMetadata metadata) {
        return metadata.getTableName();
    }

    private String generateInsertSql(String tableName, ConsumerMetadata metadata) {
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(tableName).append(" (");

        for (int i = 0; i < metadata.getFields().size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append(metadata.getFields().get(i).getTargetName());
        }

        sql.append(") VALUES (");

        for (int i = 0; i < metadata.getFields().size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append("?");
        }

        sql.append(")");
        return sql.toString();
    }

    @SuppressWarnings("all")
    private void executeBatch(List<T> batch, String sql, List<ConsumerMetadata.ConsumerFieldModel> fields) {

        TransactionTemplate transactionTemplate = transactionTemplateRef.get();
        JdbcTemplate jdbcTemplate = jdbcTemplateRef.get();
        // 防御性检查
        if (transactionTemplate == null) {
            throw new IllegalStateException("TransactionTemplate not initialized");
        }
        transactionTemplate.execute(status -> {
            try {
                jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement ps, int i) throws SQLException {
                        T item = batch.get(i);
                        for (int j = 0; j < fields.size(); j++) {
                            ConsumerMetadata.ConsumerFieldModel field = fields.get(j);
                            Object value = ReflectionUtils.getFieldValue(item, field.getFieldName());
                            ps.setObject(j + 1, value);
                        }
                    }

                    @Override
                    public int getBatchSize() {
                        return batch.size();
                    }
                });
                return batch.size();
            } catch (Exception e) {
                status.setRollbackOnly();
                throw new SyncException("JDBC batch write failed", e);
            }
        });
    }
}
