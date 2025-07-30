package kd.data.core.model;

import kd.data.core.enums.Status;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
/**
 * 统计模型
 * @author gaozw
 * @date 2025/7/23 18:44
 */

@Component
@Slf4j
@SuppressWarnings("unused")
@Data
public class SyncStats {


    private volatile Status status = Status.NOT_STARTED;

    private long startTime;

    private long duration;

    private long totalRecords;

    private int shardCount;

    // 添加失败批次上限
    private int maxFailedBatches = 1000;

    private final AtomicLong scannedRecords = new AtomicLong(0);
    private final AtomicLong filteredRecords = new AtomicLong(0);
    private final AtomicLong processedRecords = new AtomicLong(0);
    private final AtomicLong completedShards = new AtomicLong(0);
    private final AtomicLong failedShards = new AtomicLong(0);
    private final AtomicLong skippedShards = new AtomicLong(0);
    private final AtomicLong retryCount = new AtomicLong(0);

    private final ConcurrentHashMap<Integer, Long> shardDurations = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, Long> batchTimes = new ConcurrentHashMap<>();
    private final List<Object> failedBatches = new CopyOnWriteArrayList<>();


    public void startShard(int shardId) {
        shardDurations.put(shardId, System.currentTimeMillis());
    }

    public void completeShard(int shardId) {
        long start = shardDurations.get(shardId);
        long dur = System.currentTimeMillis() - start;
        shardDurations.put(shardId, dur);
        completedShards.incrementAndGet();
    }

    public void failShard() {
        failedShards.incrementAndGet();
    }

    public void incrementScannedRecords() {
        scannedRecords.incrementAndGet();
    }

    public void incrementFilteredRecords() {
        filteredRecords.incrementAndGet();
    }

    public void incrementProcessedRecords(long count) {
        processedRecords.addAndGet(count);
    }

    public void incrementSkippedShards() {
        skippedShards.incrementAndGet();
    }

    public void incrementRetryCount() {
        retryCount.incrementAndGet();
    }

    public void recordBatchTime(int shardId, long time) {
        batchTimes.put(shardId, time);
    }



    public void addFailedBatch(Object batch) {
        if (failedBatches.size() < maxFailedBatches) {
            failedBatches.add(batch);
        } else {
            log.warn("Failed batches storage full");
        }
    }
}
