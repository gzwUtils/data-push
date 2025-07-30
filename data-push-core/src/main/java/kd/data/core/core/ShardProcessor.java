package kd.data.core.core;


import kd.data.core.coordinator.DistributedCoordinator;
import kd.data.core.data.DataAccessor;
import kd.data.core.exception.SyncException;
import kd.data.core.model.SyncConfig;
import kd.data.core.model.SyncStats;
import kd.data.core.utils.CompressionUtil;
import lombok.extern.slf4j.Slf4j;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
/**
 * 分片处理器
 * @author gaozw
 * @date 2025/7/23 17:21
 */
@Slf4j
@SuppressWarnings("unused")
public class ShardProcessor<T> {

    private final SyncConfig config;
    private final Consumer<List<T>> batchConsumer;
    private final SyncStats stats;

    private final DistributedCoordinator distributedCoordinator;



    public ShardProcessor(SyncConfig config,
                          Consumer<List<T>> batchConsumer,
                          SyncStats stats,
                          DistributedCoordinator distributedCoordinator) {
        this.config = config;
        this.batchConsumer = batchConsumer;
        this.stats = stats;
        this.distributedCoordinator = distributedCoordinator;
    }

    public void processShard(int shardId, int totalShards,
                             String checkpoint, DataAccessor<T> dataAccessor) {
        long startTime = System.currentTimeMillis();
        stats.startShard(shardId);

        try (DataAccessor.Cursor<T> cursor = dataAccessor.openCursor(shardId, totalShards, checkpoint)) {
            List<T> buffer = new ArrayList<>(config.getBatchSize());
            int recordsProcessed = 0;

            while (cursor.hasNext()) {
                T re = cursor.next();
                stats.incrementScannedRecords();

                if (shouldFilter(re,dataAccessor)) {
                    stats.incrementFilteredRecords();
                    continue;
                }

                buffer.add(re);

                if (buffer.size() >= config.getBatchSize()) {
                    processBatch(buffer, shardId);
                    recordsProcessed += buffer.size();
                    buffer.clear();
                    updateCheckpoint(shardId, re, dataAccessor);
                }
            }

            if (!buffer.isEmpty()) {
                processBatch(buffer, shardId);
                recordsProcessed += buffer.size();
                updateCheckpoint(shardId, buffer.get(buffer.size() - 1), dataAccessor);
            }

            stats.completeShard(shardId);
            stats.getShardDurations().put(shardId, System.currentTimeMillis() - startTime);
            log.info("handler::recordsProcessed:{}",recordsProcessed);
        } catch (Exception e) {
            stats.failShard();
            log.error("processShard error {}",e.getMessage(),e);
            throw new SyncException("Shard processing failed: " + shardId, e);
        }
    }

    private void processBatch(List<T> batch, int shardId) {
        long startTime = System.currentTimeMillis();


        if (config.isUseCompression() && batch.size() > config.getCompressionThreshold()) {
            byte[] compressedBatch = CompressionUtil.compressBatch(batch);
            processCompressedBatch(compressedBatch, shardId);
        } else {
            processBatchWithRetry(batch,shardId);
        }

        stats.incrementProcessedRecords(batch.size());
        stats.recordBatchTime(shardId, System.currentTimeMillis() - startTime);
    }

    private void processBatchWithRetry(List<T> batch, int shardId) {
        int retryCount = 0;
        boolean success = false;
        long backoff = config.getRetryInitialDelay();

        while (!success && retryCount <= config.getMaxRetries()) {
            try {
                batchConsumer.accept(batch);
                success = true;
            } catch (Exception e) {
                retryCount++;
                stats.incrementRetryCount();
                log.error("Batch processing failed (shard {}), retry {}/{}",
                        shardId, retryCount, config.getMaxRetries(), e);
                if (retryCount > config.getMaxRetries()) {
                    handleFailedBatch(batch,shardId);
                    throw new SyncException("Batch processing failed after retries", e);
                }

                waitForRetry(backoff);
                backoff *= (long) config.getRetryBackoffFactor();
            }
        }
    }

    private void processCompressedBatch(byte[] compressedBatch, int shardId) {
        // 这里需要特殊处理压缩数据的消费者
        // 示例：batchConsumer.accept(Collections.singletonList(compressedBatch))
        log.info("Processing compressed batch of size {} bytes for shard {}",
                compressedBatch.length, shardId);
    }

    private void updateCheckpoint(int shardId, T re, DataAccessor<T> dataAccessor) {
        String shardKey = "shard_" + shardId;
        String checkpoint = dataAccessor.getRecordId(re);
        distributedCoordinator.saveCheckpoint(shardKey,checkpoint);
    }

    private boolean shouldFilter(T re,DataAccessor<T> dataAccessor) {
        return !dataAccessor.getFilterKey(re).isEmpty();
    }

    private void waitForRetry(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void handleFailedBatch(List<T> batch, int shardId) {
        log.error("Batch failed after retries (shard {}), size: {}", shardId, batch.size());
        stats.addFailedBatch(batch);
    }
}
