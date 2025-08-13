package kd.data.core.send.adapter.dialect.impl;

import kd.data.core.send.adapter.dialect.DatabaseDialect;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 默认数据库方言实现
 * @author gaozw
 * @date 2025/7/24 16:00
 */

public class DefaultDialect implements DatabaseDialect {
    @Override
    public PreparedStatement prepareStatement(Connection conn, String sql) throws SQLException {
        return conn.prepareStatement(sql);
    }

    @Override
    public void configureStreaming(PreparedStatement stmt) throws SQLException {
        stmt.setFetchSize(1000); // 默认批次大小
    }

    @Override
    public String getShardingCondition(int shardId, int totalShards, String checkpointColumn) {
        return "MOD(ABS(" + checkpointColumn + "), " + totalShards + ") = " + shardId;
    }

    @Override
    public String getRowIdCondition(String checkpointColumn, String checkpoint) {
        return checkpointColumn + " > '" + checkpoint + "'";
    }
}
