package kd.data.core.data.factory.impl;

import kd.data.core.data.DataAccessor;
import kd.data.core.data.adapter.JdbcDataAccessorAdapter;
import kd.data.core.data.adapter.dialect.DatabaseDialect;
import kd.data.core.data.adapter.dialect.DatabaseDialectFactory;
import kd.data.core.data.factory.DataAccessorFactory;
import kd.data.core.model.SyncConfig;

import javax.sql.DataSource;

/**
 * JDBC数据访问器工厂实现
 * @author gaozw
 * @date 2025/7/24 15:54
 */
@SuppressWarnings("unused")
public class JdbcDataAccessorFactory<T> implements DataAccessorFactory<T> {

    private final Class<T> entityType;
    private final DatabaseDialectFactory dialectFactory;

    public JdbcDataAccessorFactory(Class<T> entityType, DatabaseDialectFactory dialectFactory) {
        this.entityType = entityType;
        this.dialectFactory = dialectFactory;
    }

    @Override
    public DataAccessor<T> createDataAccessor(DataSource dataSource, SyncConfig config) {
        DatabaseDialect dialect = dialectFactory.createDialect(dataSource);
        return new JdbcDataAccessorAdapter<>(dataSource, entityType, dialect, config);
    }

}
