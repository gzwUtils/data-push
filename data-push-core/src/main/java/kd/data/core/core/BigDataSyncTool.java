package kd.data.core.core;
import kd.data.core.coordinator.DistributedCoordinator;
import kd.data.core.send.DataAccessor;
import kd.data.core.model.enums.Status;
import kd.data.core.exception.SyncException;
import kd.data.core.model.SyncConfig;
import kd.data.core.model.SyncStats;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.NotNull;
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


    @Setter
    private Runnable onCompleteCallback;

    private volatile boolean userStopped = false;

    private final SyncConfig config;
    private final DataAccessor<T> dataAccessor;
    private final Consumer<List<T>> batchConsumer;
    private final DistributedCoordinator coordinator;
    @Getter
    private final SyncStats stats = new SyncStats();
    private final ExecutorService executor;

    private final String taskId;

    public BigDataSyncTool(String taskId,SyncConfig config,
                           DataAccessor<T> dataAccessor,
                           Consumer<List<T>> batchConsumer,
                           DistributedCoordinator coordinator) {
        this.taskId = taskId;
        this.config = config;
        this.dataAccessor = dataAccessor;
        this.batchConsumer = batchConsumer;
        this.coordinator = coordinator;

        dataAccessor.init(config);
        this.executor = createExecutor();
    }


    public void startCallBackSync() {
        try {
            startSync();
        } finally {
            // 任务结束时触发回调
            if (onCompleteCallback != null) {
                onCompleteCallback.run();
            }
        }
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
                    .exceptionally(ex -> {
                        log.warn("Global completion exception", ex);
                        throw new SyncException("Global completion exception",ex);
                    })
                    .get(config.getGlobalTimeout(), TimeUnit.SECONDS);

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

            // 只有在没有被用户手动停止的情况下才标记为 COMPLETED
            if (!userStopped && stats.getStatus() == Status.RUNNING) {
                stats.setStatus(Status.COMPLETED);
            }
            printStats();
        }
    }


    public void stopSync() {
        userStopped = true;
        executor.shutdown();
        stats.setStatus(Status.STOPPED);
        log.info("Sync is stoPing...... by user request");
    }


    private void processShard(int shardId, int totalShards, ShardProcessor<T> processor) {


        String lockKey = taskId +":"+"shard_" + shardId;

        if (!coordinator.tryLock(lockKey, config.getLockTimeout())) {
            log.error("Shard {} skipped (already processing) ", shardId);
            stats.incrementSkippedShards();
            return;
        }

        LockRenewalService renewalService = null;
        try {
            // 启动带生命周期的续期服务
            renewalService = new LockRenewalService(coordinator, lockKey, config);
            renewalService.start();

            String checkpoint = coordinator.loadCheckpoint(lockKey);
            log.info("Processing shard {} from checkpoint: {}", shardId, checkpoint);

            processor.processShard(lockKey,shardId, totalShards, checkpoint, dataAccessor);
            String maxCheckpointInShard = dataAccessor.getMaxCheckpointInShard(shardId, totalShards);
            coordinator.saveCheckpoint(lockKey, maxCheckpointInShard);
            log.info("Shard {} completed", shardId);
        } catch (Throwable e) {
            log.error("Shard {} failed: {}", shardId, e.getMessage());
            stats.failShard();
            throw new SyncException("Shard processing failed: " + shardId, e);
        } finally {
            // 先停止续期再释放锁（关键顺序！）
            if (renewalService != null) {
                renewalService.stop();
            }
            coordinator.unlock(lockKey);
        }
    }


    // 封装续期服务
    private static class LockRenewalService {
        private final ScheduledExecutorService scheduler;
        private final LockRenewalTask task;

        private final SyncConfig config;

        public LockRenewalService(DistributedCoordinator coordinator,
                                  String lockKey,
                                  SyncConfig config) {
            this.config = config;
            //1. 使用自定义线程工厂
            this.scheduler = new ScheduledThreadPoolExecutor(1,getFactory(lockKey));

            //2.设置拒绝策略
            ((ScheduledThreadPoolExecutor) this.scheduler).setRejectedExecutionHandler(
                    new ThreadPoolExecutor.CallerRunsPolicy());

            // 3. 添加取消策略（// 及时移除已取消任务）
            ((ScheduledThreadPoolExecutor) this.scheduler).setRemoveOnCancelPolicy(true);

            this.task = new LockRenewalTask(coordinator, lockKey, config);

        }

        private static ThreadFactory getFactory(String lockKey) {
            return new ThreadFactory() {
                private final AtomicInteger counter = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    if (r == null) {
                        throw new SyncException("Runnable cannot be null");
                    }
                    Thread t = new Thread(r, "lock-renewal-" + lockKey + "-" + counter.incrementAndGet());
                    t.setDaemon(true);  // 守护线程
                    t.setUncaughtExceptionHandler((thread, ex) ->
                            log.error("Thread {} failed: {}", thread.getName(), ex.getMessage()));
                    return t;
                }
            };
        }

        public void start() {
            // 验证时间配置合理性
            if (config.getLockRenewInterval() >= config.getLockTimeout() * 0.75) {
                throw new IllegalArgumentException("续期间隔必须小于锁超时时间的75%");
            }

            scheduler.scheduleAtFixedRate(
                    task,
                    config.getLockRenewInterval(),
                    config.getLockRenewInterval(),
                    TimeUnit.SECONDS
            );
        }

        public void stop() {
            scheduler.shutdownNow(); // 立即中断任务
            try {
                if (!scheduler.awaitTermination(3, TimeUnit.SECONDS)) {
                    log.warn("Lock renewal thread shutdown timeout for {}", task.lockKey);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while stopping lock renewal");
            }
        }
    }

    // 续期任务增加中断处理
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
            try {
                if (Thread.interrupted()) {
                    // 响应中断
                    return;
                }
                if (!coordinator.renewLock(lockKey, config.getLockTimeout())) {
                    log.error("锁续期失败: {}", lockKey);
                }
            } catch (Exception e) {
                log.error("续期任务异常", e);
            }
        }
    }

    private int calculateShardCount(long totalRecords) {
        long recordsPerShard = Math.max(1, config.getRecordsPerShard());

        int minShards = Math.max(1, config.getMinShards());
        int maxShards = Math.max(minShards, config.getMaxShards());

        /* 1. 按记录量粗算 */
        int recordBased = (int) Math.ceil((double) totalRecords / recordsPerShard);

        /* 2. CPU 维度：至少给 1 倍核心，最多给 4 倍核心，避免大机爆炸 */
        int cpuBased = Math.min(
                Math.max(1, Math.min(Runtime.getRuntime().availableProcessors() * 2, 256)),
                maxShards);

        /* 3. 内存维度：留 50% 安全垫 */
        int  memoryBased = Math.min(
                Math.max(1, calculateMemoryBasedShards()),
                maxShards);

        /* 4. 多层钳位：recordBased ↑，cpuBased ↑，memoryBased ↓ */
        int shards = Math.max(recordBased, cpuBased);
        shards = Math.min(shards, memoryBased);
        return Math.max(minShards, shards);
    }

    private int calculateMemoryBasedShards() {
        long maxHeap = Runtime.getRuntime().maxMemory();
        long batchSize = Math.max(1, config.getBatchSize());

        // 每条记录占内存 = 对象头(16) + 字段(平均 100)  ≈ 120 B
        long bytesPerRecord = 120L;
        long bytesPerShard  = batchSize * bytesPerRecord;

        // 保守：每片占 2×内存（读缓冲 + 写缓冲 + 临时对象）
        long neededPerShard = bytesPerShard * 2;

        // 给堆外、GC、其他线程再留 50%
        long upper = maxHeap / 2;
        return (int) (upper / neededPerShard);
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
        @SuppressWarnings("all")
        public Thread newThread(@NotNull Runnable r) {
            Thread thread = new Thread(r, "sync-worker-" + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        }
    }


}
