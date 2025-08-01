package kd.data.core.customer.target.impl.jdbc;

import kd.data.core.customer.target.TargetConnector;
import kd.data.core.customer.target.targetenums.TargetEnums;
import lombok.Getter;
import org.springframework.jdbc.core.JdbcTemplate;
import javax.sql.DataSource;

/**
 * JDBC目标连接器
 * @author gaozw
 * @date 2025/7/24 16:36
 */
@SuppressWarnings("all")
@Getter
public class JdbcTargetConnector implements TargetConnector {

    private final DataSource dataSource;

    private JdbcTemplate jdbcTemplate;

    private boolean connected = false;


    private static final String TEST_CONNECTION = "SELECT 1";

    public JdbcTargetConnector(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void connect() {
        if (!isConnected()) {
            this.jdbcTemplate = new JdbcTemplate(dataSource);
            // 测试连接
            jdbcTemplate.execute(TEST_CONNECTION);
            connected = true;
        }
    }

    @Override
    public boolean isConnected() {
        return connected;
    }

    @Override
    public void close() {
        // 通常由连接池管理
        connected = false;
    }

    @Override
    public String getType() {
        return TargetEnums.JDBC.name();
    }

}
