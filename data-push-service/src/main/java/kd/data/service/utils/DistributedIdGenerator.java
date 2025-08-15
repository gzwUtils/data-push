package kd.data.service.utils;

import kd.data.service.entity.IdMeta;
import kd.data.service.exception.TaskException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import javax.annotation.PostConstruct;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 分布式ID生成器（优化版）
 * 改进点：
 * 1. 使用ReentrantLock替代synchronized，提高并发性能
 * 2. 增加时钟回拨的多种处理策略
 * 3. 添加详细的性能统计指标
 * 4. 支持ID生成速率限制
 * 5. 提供更详细的状态监控接口
 * 6. 解决构造函数参数过多问题
 *
 * @author gaozw
 * @date 2025/8/13 11:50
 */
@SuppressWarnings("unused")
@Slf4j
@Component
public class DistributedIdGenerator {

    // ================== 配置参数 ==================
    @Value("${distributed.id.epoch:1672531200000}")  // 2023-01-01
    private long epoch;

    @Value("${distributed.id.worker-id-bits:10}")
    private long workerIdBits;

    @Value("${distributed.id.sequence-bits:12}")
    private long sequenceBits;

    @Value("${distributed.id.worker-id:1}")
    private long workerId;

    @Value("${distributed.id.max-clock-backward-ms:100}")
    private long maxClockBackwardMs;

    @Value("${distributed.id.max-sequence-wait-ms:10}")  // 序列号耗尽等待时间
    private long maxSequenceWaitMs;

    @Value("${distributed.id.max-ids-per-second:0}")  // 限流：0表示不限流
    private long maxIdsPerSecond;

    // ================== 计算字段 ==================
    private long maxWorkerId;
    private long maxSequence;
    private long timestampShift;
    private long workerIdShift;

    // ================== 运行时状态 ==================
    private long lastTimestamp = -1L;
    private long sequence = 0L;
    private final Lock lock = new ReentrantLock();

    // ================== 统计指标 ==================
    @Getter
    private final Stats stats = new Stats();

    // 限流相关
    private long lastRateCheckTime = System.currentTimeMillis();
    private long idsGeneratedInCurrentSecond = 0;

    /**
     * 统计指标内部类
     */
    @Getter
    public static class Stats {
        private final AtomicLong clockBackwardCount = new AtomicLong(0L);
        private final AtomicLong totalGeneratedIds = new AtomicLong(0L);
        private final AtomicLong sequenceExhaustedCount = new AtomicLong(0L);
        private final AtomicLong waitTimeForClockBackward = new AtomicLong(0L);
        private final AtomicLong waitTimeForSequence = new AtomicLong(0L);
    }

    @PostConstruct
    public void init() {
        validateAndCalculateParameters();
        log.info("分布式ID生成器初始化完成. Epoch: {}, Worker ID: {}, Worker ID Bits: {}, Sequence Bits: {}, Max IDs/s: {}",
                epoch, workerId, workerIdBits, sequenceBits, maxIdsPerSecond);
    }

    private void validateAndCalculateParameters() {
        // 验证并计算参数
        maxWorkerId = ~(-1L << workerIdBits);
        maxSequence = ~(-1L << sequenceBits);
        workerIdShift = sequenceBits;
        timestampShift = sequenceBits + workerIdBits;

        // 参数验证
        if (workerId > maxWorkerId || workerId < 0) {
            throw new IllegalArgumentException(
                    String.format("Worker ID 必须在0和%d之间", maxWorkerId));
        }

        if (maxClockBackwardMs <= 0) {
            throw new IllegalArgumentException("最大时钟回拨时间必须大于0");
        }
    }

    /**
     * 生成下一个唯一ID
     */
    public long nextId() {
        // 限流检查
        applyRateLimit();

        lock.lock();
        try {
            long timestamp = timeGen();

            // 时钟回拨处理
            timestamp = handleClockBackward(timestamp);

            // 序列号处理
            if (lastTimestamp == timestamp) {
                sequence = (sequence + 1) & maxSequence;
                if (sequence == 0) {
                    // 序列号耗尽处理
                    stats.sequenceExhaustedCount.incrementAndGet();
                    long startWait = System.currentTimeMillis();
                    timestamp = waitForNextMillis(lastTimestamp);
                    stats.waitTimeForSequence.addAndGet(System.currentTimeMillis() - startWait);
                }
            } else {
                sequence = 0L;
            }

            lastTimestamp = timestamp;
            stats.totalGeneratedIds.incrementAndGet();
            idsGeneratedInCurrentSecond++;

            return ((timestamp - epoch) << timestampShift)
                    | (workerId << workerIdShift)
                    | sequence;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 处理时钟回拨情况
     */
    private long handleClockBackward(long currentTimestamp) {
        if (currentTimestamp < lastTimestamp) {
            long offset = lastTimestamp - currentTimestamp;
            stats.clockBackwardCount.incrementAndGet();

            if (offset <= maxClockBackwardMs) {
                log.warn("检测到时钟回拨 {}ms，等待中...", offset);
                try {
                    long startWait = System.currentTimeMillis();
                    TimeUnit.MILLISECONDS.sleep(offset);
                    stats.waitTimeForClockBackward.addAndGet(System.currentTimeMillis() - startWait);
                    currentTimestamp = timeGen();

                    // 再次检查是否已恢复
                    if (currentTimestamp < lastTimestamp) {
                        throw new TaskException(String.format("时钟回拨处理失败，回拨时间: %dms", offset));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new TaskException("等待时钟回拨时被中断", e);
                }
            } else {
                throw new TaskException(String.format("时钟回拨过大，拒绝生成ID. 回拨时间: %dms", offset));
            }
        }
        return currentTimestamp;
    }

    /**
     * 等待直到下一毫秒
     */
    private long waitForNextMillis(long lastTimestamp) {
        long timestamp = timeGen();
        long startWait = System.currentTimeMillis();

        while (timestamp <= lastTimestamp) {
            if (System.currentTimeMillis() - startWait > maxSequenceWaitMs) {
                throw new TaskException("等待新时间窗口超时");
            }
            timestamp = timeGen();
        }
        return timestamp;
    }

    /**
     * 应用速率限制
     */
    private void applyRateLimit() {
        if (maxIdsPerSecond <= 0) return;

        long currentTime = System.currentTimeMillis();
        long elapsed = currentTime - lastRateCheckTime;

        if (elapsed >= 1000) {
            // 重置计数器
            lastRateCheckTime = currentTime;
            idsGeneratedInCurrentSecond = 0;
        } else if (idsGeneratedInCurrentSecond >= maxIdsPerSecond) {
            // 计算需要等待的时间
            long waitTime = 1000 - elapsed;
            try {
                TimeUnit.MILLISECONDS.sleep(waitTime);
                lastRateCheckTime = System.currentTimeMillis();
                idsGeneratedInCurrentSecond = 0;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new TaskException("限流等待时被中断", e);
            }
        }
    }

    /**
     * 获取当前时间戳
     */
    protected long timeGen() {
        return System.currentTimeMillis();
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

    /**
     * 获取生成器状态
     */
    public GeneratorStatus getStatus() {
        return new GeneratorStatus(this);
    }

    /**
     * 生成器状态对象
     */
    @Getter
    public static class GeneratorStatus {
        private final long workerId;
        private final long lastTimestamp;
        private final long sequence;
        private final long totalIds;
        private final long clockBackwardCount;
        private final long sequenceExhaustedCount;
        private final long totalClockBackwardWaitTime;
        private final long totalSequenceWaitTime;
        private final String lastTimestampAsString;

        public GeneratorStatus(DistributedIdGenerator generator) {
            this.workerId = generator.workerId;
            this.lastTimestamp = generator.lastTimestamp;
            this.sequence = generator.sequence;
            this.totalIds = generator.stats.totalGeneratedIds.get();
            this.clockBackwardCount = generator.stats.clockBackwardCount.get();
            this.sequenceExhaustedCount = generator.stats.sequenceExhaustedCount.get();
            this.totalClockBackwardWaitTime = generator.stats.waitTimeForClockBackward.get();
            this.totalSequenceWaitTime = generator.stats.waitTimeForSequence.get();
            this.lastTimestampAsString = lastTimestamp > 0 ?
                    Instant.ofEpochMilli(lastTimestamp).toString() : "N/A";
        }
    }
}