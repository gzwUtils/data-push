package kd.data.service.utils;
import kd.data.service.entity.IdMeta;
import kd.data.service.exception.TaskException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.concurrent.TimeUnit;

/**
 * 分布式id
 *
 * @author gaozw
 * @date 2025/8/13 11:50
 */
@SuppressWarnings("unused")
@Slf4j
@Component
public class DistributedIdGenerator {

    // 起始时间戳
    @Value("${distributed.id.epoch:1672531200000}")
    private long epoch;

    // 工作节点ID位数
    @Value("${distributed.id.worker-id-bits:10}")
    private long workerIdBits;

    // 序列号位数
    @Value("${distributed.id.sequence-bits:12}")
    private long sequenceBits;

    // 工作节点ID
    @Value("${distributed.id.worker-id:1}")
    private long workerId;

    // 最大容忍时钟回拨时间(ms)
    @Value("${distributed.id.max-clock-backward-ms:100}")
    private long maxClockBackwardMs;

    // 最大工作节点ID
    private long maxWorkerId;
    // 最大序列号
    private long maxSequence;
    // 时间戳左移位数
    private long timestampShift;
    // 工作节点ID左移位数
    private long workerIdShift;
    // 上次生成ID的时间戳
    private long lastTimestamp = -1L;
    // 序列号
    private long sequence = 0L;
    /**
     * -- GETTER --
     *  获取时钟回拨次数
     */
    // 时钟回拨次数
    @Getter
    private long clockBackwardCount = 0L;

    @PostConstruct
    public void init() {
        // 计算最大值
        maxWorkerId = ~(-1L << workerIdBits);
        maxSequence = ~(-1L << sequenceBits);

        // 计算移位偏移量
        workerIdShift = sequenceBits;
        timestampShift = sequenceBits + workerIdBits;

        // 验证工作节点ID
        if (workerId > maxWorkerId || workerId < 0) {
            throw new IllegalArgumentException(
                    String.format("Worker ID 必须在0和%d之间", maxWorkerId));
        }

        log.info("分布式ID生成器初始化完成. Epoch: {}, Worker ID: {}, Worker ID Bits: {}, Sequence Bits: {}",
                epoch, workerId, workerIdBits, sequenceBits);
    }

    /**
     * 生成下一个唯一ID
     */
    public synchronized long nextId() {
        long timestamp = timeGen();

        // 时钟回拨处理
        if (timestamp < lastTimestamp) {
            long offset = lastTimestamp - timestamp;
            if (offset <= maxClockBackwardMs) {
                log.warn("检测到时钟回拨 {}ms，等待中...", offset);
                clockBackwardCount++;
                try {
                    TimeUnit.MILLISECONDS.sleep(offset);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new TaskException("等待时钟回拨时被中断", e);
                }
                timestamp = timeGen();
                if (timestamp < lastTimestamp) {
                    throw new TaskException(String.format("时钟回拨处理失败，回拨时间: %dms", offset));
                }
            } else {
                throw new TaskException(String.format("时钟回拨过大，拒绝生成ID. 回拨时间: %dms", offset));
            }
        }

        // 同一毫秒内生成
        if (lastTimestamp == timestamp) {
            sequence = (sequence + 1) & maxSequence;
            if (sequence == 0) {
                timestamp = tilNextMillis(lastTimestamp);
            }
        } else {
            sequence = 0L;
        }

        lastTimestamp = timestamp;

        return ((timestamp - epoch) << timestampShift)
                | (workerId << workerIdShift)
                | sequence;
    }

    /**
     * 获取当前时间戳
     */
    protected long timeGen() {
        return System.currentTimeMillis();
    }

    /**
     * 阻塞到下一毫秒
     */
    protected long tilNextMillis(long lastTimestamp) {
        long timestamp = timeGen();
        while (timestamp <= lastTimestamp) {
            timestamp = timeGen();
        }
        return timestamp;
    }

    /**
     * 解析ID
     */
    public IdMeta parseId(long id) {
        long timestamp = (id >> timestampShift) + epoch;
        long newWorkerId = (id >> workerIdShift) & maxWorkerId;
        long newSequence = id & maxSequence;

        return new IdMeta(id, timestamp, newWorkerId, newSequence);
    }

}

