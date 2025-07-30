package kd.data.core.data.adapter.dialect;

import javax.sql.DataSource;

/**
 * 数据库方言工厂接口
 * @author gaozw
 * @date 2025/7/24 15:56
 */

public interface DatabaseDialectFactory {

    DatabaseDialect createDialect(DataSource dataSource);
}
