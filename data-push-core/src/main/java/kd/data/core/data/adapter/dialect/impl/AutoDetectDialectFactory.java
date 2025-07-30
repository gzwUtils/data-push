package kd.data.core.data.adapter.dialect.impl;

import kd.data.core.data.adapter.dialect.DatabaseDialect;
import kd.data.core.data.adapter.dialect.DatabaseDialectFactory;
import kd.data.core.data.adapter.dialect.impl.mysql.MySQLDialect;
import kd.data.core.exception.SyncException;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * 自动检测数据库方言的工厂
 * @author gaozw
 * @date 2025/7/24 15:57
 */
@SuppressWarnings("unused")
@Slf4j
public class AutoDetectDialectFactory implements DatabaseDialectFactory {

    private static final Map<String, DatabaseDialect> DIALECT_MAP = new HashMap<>();

    static {
        // 注册支持的数据库方言
        registerDialect("MySQL", new MySQLDialect());
    }

    public static void registerDialect(String dbName, DatabaseDialect dialect) {
        DIALECT_MAP.put(dbName, dialect);
    }

    @Override
    public DatabaseDialect createDialect(DataSource dataSource) {
        try (Connection conn = dataSource.getConnection()) {
            String dbName = conn.getMetaData().getDatabaseProductName();
            DatabaseDialect dialect = DIALECT_MAP.get(dbName);

            if (dialect == null) {
                dialect = new DefaultDialect();
                log.warn("Using default dialect for unsupported database: {}", dbName);
            }

            log.info("Detected database dialect: {}", dialect.getClass().getSimpleName());
            return dialect;
        } catch (SQLException e) {
            throw new SyncException("Failed to detect database dialect", e);
        }
    }
}
