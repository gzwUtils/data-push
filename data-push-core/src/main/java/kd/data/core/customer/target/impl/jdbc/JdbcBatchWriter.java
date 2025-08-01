package kd.data.core.customer.target.impl.jdbc;

import kd.data.core.customer.meta.ConsumerMetadata;
import kd.data.core.customer.target.TargetConnector;
import kd.data.core.customer.target.TargetWriter;
import kd.data.core.exception.SyncException;
import kd.data.core.utils.ReflectionUtils;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * @author gaozw
 * @date 2025/7/24 16:43
 */
@SuppressWarnings({"unused"})
public class JdbcBatchWriter<T> implements TargetWriter<T> {

    private JdbcTemplate jdbcTemplate;
    private TransactionTemplate transactionTemplate;

    @Override
    public void write(List<T> batch, TargetConnector connector, ConsumerMetadata metadata) {
        if (!(connector instanceof JdbcTargetConnector)) {
            throw new IllegalArgumentException("Invalid connector for JDBC writer");
        }

        // 延迟初始化
        if (jdbcTemplate == null) {
            DataSource dataSource = ((JdbcTargetConnector) connector).getDataSource();
            this.jdbcTemplate = new JdbcTemplate(dataSource);
            this.transactionTemplate = new TransactionTemplate(
                    new DataSourceTransactionManager(dataSource)
            );
        }

        // 生成插入SQL
        String tableName = getTableName(metadata);
        String sql = generateInsertSql(tableName, metadata);

        // 执行批量写入
        executeBatch(batch, sql, metadata.getFields());
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

    private void executeBatch(List<T> batch, String sql, List<ConsumerMetadata.ConsumerFieldModel> fields) {
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
