package kd.data.core.send.factory;

import kd.data.core.send.DataAccessor;
import kd.data.core.model.SyncConfig;

import javax.sql.DataSource;

/**
 * 数据访问器工厂接口
 *
 * @author gaozw
 * @date 2025/7/24 15:53
 */
@SuppressWarnings("unused")
public interface DataAccessorFactory<T> {

    DataAccessor<T> createDataAccessor(DataSource dataSource, SyncConfig config);
}
