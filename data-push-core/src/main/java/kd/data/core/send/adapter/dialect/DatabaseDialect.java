package kd.data.core.send.adapter.dialect;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 数据库方言接口
 * @author gaozw
 * @date 2025/7/23 19:42
 */

public interface DatabaseDialect {

    PreparedStatement prepareStatement(Connection conn, String sql) throws SQLException;
    void configureStreaming(PreparedStatement stmt) throws SQLException;
    String getShardingCondition(int shardId, int totalShards, String checkpointColumn);
    String getRowIdCondition(String checkpointColumn, String checkpoint);
}
