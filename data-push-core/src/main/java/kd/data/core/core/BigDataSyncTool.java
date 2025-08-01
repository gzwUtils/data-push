package kd.data.core.core;
import kd.data.core.coordinator.DistributedCoordinator;
import kd.data.core.data.DataAccessor;
import kd.data.core.enums.Status;
import kd.data.core.exception.SyncException;
import kd.data.core.model.SyncConfig;
import kd.data.core.model.SyncStats;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;


/**
 * 海量数据同步引擎 - 支持亿万级数据同步
 * 核心特性：
 * 1. 分布式分片处理
 * 2. 游标分页读取
 * 3. 生产者-消费者模式
 * 4. 智能重试机制
 * 5. 实时监控统计
 * @author gaozw
 * @date 2025/7/23 17:20
 */
@SuppressWarnings("unused")
@Slf4j
public class BigDataSyncTool<T> {

    private final SyncConfig config;
    private final DataAccessor<T> dataAccessor;
    private final Consumer<List<T>> batchConsumer;
    private final DistributedCoordinator coordinator;
    @Getter
    private final SyncStats stats = new SyncStats();
    private final ExecutorService executor;

    public BigDataSyncTool(SyncConfig config,
                           DataAccessor<T> dataAccessor,
                           Consumer<List<T>> batchConsumer,
                           DistributedCoordinator coordinator) {
        this.config = config;
        this.dataAccessor = dataAccessor;
        this.batchConsumer = batchConsumer;
        this.coordinator = coordinator;

        dataAccessor.init(config);
        this.executor = createExecutor();
    }

    public void startSync() {

        long startTime = System.currentTimeMillis();
        stats.setStartTime(startTime);
        stats.setStatus(Status.RUNNING);

        try {
            long totalRecords = dataAccessor.countRecords();
            stats.setTotalRecords(totalRecords);

            if (totalRecords == 0) {
                log.info("No data to sync");
                stats.setStatus(Status.COMPLETED);
                return;
            }

            int shardCount = calculateShardCount(totalRecords);
            stats.setShardCount(shardCount);

            log.info("Starting sync of {} records using {} shards", totalRecords, shardCount);

            ShardProcessor<T> shardProcessor = new ShardProcessor<>(config, batchConsumer, stats,coordinator);

            List<CompletableFuture<Void>> futures = new ArrayList<>();

            for (int shardId = 0; shardId < shardCount; shardId++) {
                final int currentShard = shardId;
                futures.add(CompletableFuture.runAsync(
                        () -> processShard(currentShard, shardCount, shardProcessor),
                        executor
                ));
            }

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .get(config.getGlobalTimeout(), TimeUnit.SECONDS);

            stats.setStatus(Status.COMPLETED);

        } catch (TimeoutException e) {
            stats.setStatus(Status.TIMEOUT);
            throw new SyncException("Sync timed out", e);
        } catch (InterruptedException | ExecutionException  e) {
            // 设置状态为 STOPPED
            stats.setStatus(Status.STOPPED);

            // 重新设置中断状态（仅针对 InterruptedException）
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }

            // 抛出新异常，包含原始异常信息
            throw new SyncException("Sync Interrupted|Execution", e);
        } catch (Exception e) {
            stats.setStatus(Status.FAILED);
            throw new SyncException("Sync failed", e);
        }finally {
            executor.shutdown();
            dataAccessor.close();

            long duration = System.currentTimeMillis() - startTime;
            stats.setDuration(duration);
            printStats();
        }
    }


    public void stopSync() {
        executor.shutdownNow();
        stats.setStatus(Status.STOPPED);
        log.info("Sync is stoPing...... by user request");
    }


    private void processShard(int shardId, int totalShards, ShardProcessor<T> processor) {


        String lockKey = "shard_" + shardId;

        if (!coordinator.tryLock(lockKey, config.getLockTimeout())) {
            log.error("Shard {} skipped (already processing) ", shardId);
            stats.incrementSkippedShards();
            return;
        }

        try {
            // 启动锁续期
            LockRenewalTask renewalTask = new LockRenewalTask(coordinator, lockKey, config);
            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
            scheduler.scheduleAtFixedRate(renewalTask,
                    config.getLockRenewInterval(),
                    config.getLockRenewInterval(),
                    TimeUnit.SECONDS
            );

            String checkpoint = coordinator.loadCheckpoint(lockKey);
            log.info("Processing shard {} from checkpoint: {}", shardId, checkpoint);

            processor.processShard(shardId, totalShards, checkpoint, dataAccessor);
            String maxCheckpointInShard = dataAccessor.getMaxCheckpointInShard(shardId, totalShards);
            coordinator.saveCheckpoint(lockKey, maxCheckpointInShard);
            log.info("Shard {} completed", shardId);
            scheduler.shutdown();
        } catch (Exception e) {
            log.error("Shard {} failed: {}", shardId, e.getMessage());
            stats.failShard();
            throw new SyncException("Shard processing failed: " + shardId, e);
        } finally {
            coordinator.unlock(lockKey);
        }
    }

    private static class LockRenewalTask implements Runnable {
        private final DistributedCoordinator coordinator;
        private final String lockKey;
        private final SyncConfig config;

        public LockRenewalTask(DistributedCoordinator coordinator,
                               String lockKey, SyncConfig config) {
            this.coordinator = coordinator;
            this.lockKey = lockKey;
            this.config = config;
        }

        @Override
        public void run() {
            if (!coordinator.renewLock(lockKey, config.getLockTimeout())) {
                log.error("Lock renewal failed for shard {}", lockKey);
            }
        }
    }

    private int calculateShardCount(long totalRecords) {
        long recordsPerShard = config.getRecordsPerShard();
        int minShards = config.getMinShards();
        int maxShards = config.getMaxShards();

        int calculated = (int) Math.ceil((double) totalRecords / recordsPerShard);
        return Math.max(minShards, Math.min(maxShards, calculated));
    }

    private ExecutorService createExecutor() {
        return new ThreadPoolExecutor(
                config.getCorePoolSize(),
                config.getMaxPoolSize(),
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(config.getQueueCapacity()),
                new SyncThreadFactory()
        );
    }

    private void printStats() {
        log.info("\n====== Sync Statistics ======");
        log.info("Status: {}", stats.getStatus());
        log.info("Duration: {} minutes", stats.getDuration() / 60000.0);
        log.info("Total records: {}", stats.getTotalRecords());
        log.info("Processed records: {}", stats.getProcessedRecords().get());
        log.info("Shards: {} total, {} completed, {} failed, {} skipped",
                stats.getShardCount(), stats.getCompletedShards().get(),
                stats.getFailedShards().get(), stats.getSkippedShards().get());
        log.info("============================");
    }

    static class SyncThreadFactory implements ThreadFactory {
        private final AtomicInteger counter = new AtomicInteger(0);

        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r, "sync-worker-" + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        }
    }


}
