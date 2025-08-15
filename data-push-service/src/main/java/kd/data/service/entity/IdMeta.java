package kd.data.service.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * @author gaozw
 * @date 2025/8/13 11:59
 */
@Data
@AllArgsConstructor
public class IdMeta {
    private long id;
    private long timestamp;
    private long workerId;
    private long sequence;
    private String createTimeStr; // 新增字段

    public IdMeta(long id, long timestamp, long workerId, long sequence) {
        this.id = id;
        this.timestamp = timestamp;
        this.workerId = workerId;
        this.sequence = sequence;
        this.createTimeStr = formatCreateTime(timestamp);
    }

    private String formatCreateTime(long timestamp) {
        return timestamp > 0 ?
                Instant.ofEpochMilli(timestamp).toString() : "N/A";
    }


    public LocalDateTime getCreateTime() {
        return LocalDateTime.ofInstant(
                Instant.ofEpochMilli(timestamp),
                ZoneId.systemDefault()
        );
    }

    @Override
    public String toString() {
        return String.format(
                "ID: %d创建时间: %s工作节点: %d序列号: %d",
                id, getCreateTime(), workerId, sequence
        );
    }
}
