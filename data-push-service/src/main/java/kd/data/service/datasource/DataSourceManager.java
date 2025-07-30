package kd.data.service.datasource;

import com.zaxxer.hikari.HikariDataSource;
import javax.sql.DataSource;
import java.util.Map;

/**
 * @author gaozw
 * @date 2025/7/25 10:41
 */
public class DataSourceManager {

    private DataSourceManager(){}

    public  static DataSource getJdbcDataSource(Map<String, Object> config) {
            HikariDataSource ds = new HikariDataSource();
            ds.setJdbcUrl((String) config.get("url"));
            ds.setUsername((String) config.get("username"));
            ds.setPassword((String) config.get("password"));
            ds.setMaximumPoolSize(20);
            return ds;
    }
}
