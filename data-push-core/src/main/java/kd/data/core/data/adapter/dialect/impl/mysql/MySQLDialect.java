package kd.data.core.data.adapter.dialect.impl.mysql;

import kd.data.core.data.adapter.dialect.impl.DefaultDialect;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author MySQL数据库方言实现
 * @date 2025/7/24 16:01
 */

public class MySQLDialect extends DefaultDialect {

    @Override
    public void configureStreaming(PreparedStatement stmt) throws SQLException {
        // MySQL流式读取的特殊设置
        stmt.setFetchSize(Integer.MIN_VALUE);
    }

    @Override
    public String getShardingCondition(int shardId, int totalShards, String checkpointColumn) {
        return "MOD(" + checkpointColumn + ", " + totalShards + ") = " + shardId;
    }
}
