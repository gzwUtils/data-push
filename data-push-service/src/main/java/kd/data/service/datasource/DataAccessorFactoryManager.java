package kd.data.service.datasource;

import kd.data.core.data.DataAccessor;

import kd.data.core.data.adapter.JdbcDataAccessorAdapter;
import kd.data.core.data.adapter.dialect.DatabaseDialectFactory;
import kd.data.core.data.adapter.dialect.impl.AutoDetectDialectFactory;
import kd.data.core.model.SyncConfig;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;


/**
 * @author gaozw
 * @date 2025/7/25 10:42
 */
@Component
public class DataAccessorFactoryManager {

    public <T> DataAccessor<T> createDataAccessor(
            Class<T> entityType,
            DataSource dataSource,
            SyncConfig config) {

        // 使用JDBC数据访问器（默认实现）
        DatabaseDialectFactory dialectFactory = new AutoDetectDialectFactory();
        return new JdbcDataAccessorAdapter<>(
                dataSource,
                entityType,
                dialectFactory.createDialect(dataSource),
                config
        );
    }
}
