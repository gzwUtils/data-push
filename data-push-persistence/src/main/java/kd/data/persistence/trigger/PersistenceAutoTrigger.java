package kd.data.persistence.trigger;

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import kd.data.core.model.SyncStats;
import kd.data.core.persistence.PersistenceService;
import kd.data.persistence.ProcessContext;
import kd.data.persistence.ProcessControl;
import kd.data.persistence.ProcessModel;
import kd.data.persistence.response.R;
import kd.data.service.model.SyncTaskConfig;
import kd.data.service.task.TaskConfigCache;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static kd.data.persistence.enums.BusinessStrategyCode.PERSISTENCE;
@SuppressWarnings("all")
@Slf4j
@Component
@ConditionalOnBean(PersistenceService.class)
public class PersistenceAutoTrigger {

    private final ProcessControl processControl;
    private final TaskConfigCache taskConfigCache;

    @Autowired
    public PersistenceAutoTrigger(ProcessControl processControl,
                                  TaskConfigCache taskConfigCache) {
        this.processControl = processControl;
        this.taskConfigCache = taskConfigCache;
    }

    @Scheduled(fixedDelay = 5000)
    public void scheduledPersist() {
        int page = 0;
        int pageSize = 100;
        int processed = 0;
        int failureCount = 0;
        final int MAX_FAILURES = 3;

        try {
            while (failureCount < MAX_FAILURES) {
                List<SyncTaskConfig> batch = taskConfigCache.getTasks(page, pageSize);
                batch = batch.stream()
                        .filter(t -> !t.isPersist())
                        .collect(Collectors.toList());

                if (batch.isEmpty()) {
                    break;
                }

                for (SyncTaskConfig task : batch) {
                    if (failureCount >= MAX_FAILURES) {
                        break;
                    }

                    if (processTask(task)) {
                        processed++;
                        task.setPersist(true);
                        taskConfigCache.addTask(task);
                    } else {
                        failureCount++;
                        log.error("处理任务失败: taskId={}", task.getTaskId());
                    }
                }
                page++;
            }

            if (log.isDebugEnabled()) {
                log.debug("定时处理完成: 数量={}", processed);
            }
        } catch (Exception e) {
            log.error("定时处理异常:{}", e.getMessage(),e);
        }
    }

    @Scheduled(fixedDelay = 3000)
    public void scheduledPersistStats() {
        try {
            List<SyncStats> unpersistedStats = taskConfigCache.getUnpersistedStats();

            if (unpersistedStats.isEmpty()) {
                return;
            }

            int successCount = 0;
            List<String> persistedTaskIds = new ArrayList<>();

            for (SyncStats stats : unpersistedStats) {
                if (processStats(stats)) {
                    successCount++;
                    persistedTaskIds.add(stats.getTaskId());
                }
            }

            // 批量标记为已持久化
            if (!persistedTaskIds.isEmpty()) {
                taskConfigCache.markStatsPersisted(persistedTaskIds);
            }

            if (log.isDebugEnabled() && successCount > 0) {
                log.debug("统计信息定时持久化完成: 数量={}", successCount);
            }

        } catch (Exception e) {
            log.error("统计信息定时持久化异常", e);
        }
    }

    @PreDestroy
    public void shutdownPersist() {
        log.info("开始停机持久化...");

        // 1. 持久化所有未持久化的任务配置
        List<SyncTaskConfig> allTasks = taskConfigCache.getAllTasks();
        int configSuccessCount = 0;

        for (SyncTaskConfig task : allTasks) {
            if (processTask(task)) {
                configSuccessCount++;
                task.setPersist(true);
                taskConfigCache.addTask(task);
            }
        }

        // 2. 持久化所有未持久化的统计信息
        List<SyncStats> allStats = taskConfigCache.getAllStats();
        int statsSuccessCount = 0;
        List<String> persistedTaskIds = new ArrayList<>();

        for (SyncStats stats : allStats) {
            if (stats != null && stats.needPersist()) {
                if (processStats(stats)) {
                    statsSuccessCount++;
                    persistedTaskIds.add(stats.getTaskId());
                }
            }
        }

        // 标记统计为已持久化
        if (!persistedTaskIds.isEmpty()) {
            taskConfigCache.markStatsPersisted(persistedTaskIds);
        }

        log.info("停机处理完成: 配置={}/{}, 统计={}/{}",
                configSuccessCount, allTasks.size(),
                statsSuccessCount, allStats.size());
    }

    @Scheduled(fixedRate = 1800000)
    public void logCacheStats() {
        CacheStats stats = taskConfigCache.stats();
        log.info("缓存统计: 命中率={}%, 淘汰数={}, 加载数={}",
                stats.hitRate() * 100,
                stats.evictionCount(),
                stats.loadCount());
    }

    /**
     * 处理任务配置
     */
    private boolean processTask(SyncTaskConfig task) {
        try {
            ProcessModel model = buildTaskConfigModel(task);

            ProcessContext context = ProcessContext.builder()
                    .strategyCode(PERSISTENCE.name())
                    .model(model)
                    .needBreak(false)
                    .response(R.success())
                    .build();

            ProcessContext result = processControl.process(context);
            return !result.getNeedBreak();
        } catch (Exception e) {
            log.error("任务[{}]处理失败: {}", task.getTaskId(), e.getMessage());
            return false;
        }
    }

    /**
     * 处理统计信息
     */
    private boolean processStats(SyncStats stats) {
        try {
            ProcessModel model = buildStatsModel(stats);

            ProcessContext context = ProcessContext.builder()
                    .strategyCode(PERSISTENCE.name())
                    .model(model)
                    .needBreak(false)
                    .response(R.success())
                    .build();

            ProcessContext result = processControl.process(context);
            return !result.getNeedBreak();
        } catch (Exception e) {
            log.error("统计信息处理失败: taskId={}", stats.getTaskId(), e);
            return false;
        }
    }

    /**
     * 构建任务配置模型
     */
    private ProcessModel buildTaskConfigModel(SyncTaskConfig task) {
        return ProcessModel.builder()
                .taskId(task.getTaskId())
                .taskName(task.getTaskName())
                .desConfig(task.getDestinationConfig())
                .sourceConfig(task.getSourceConfig())
                .sourceType(task.getSourceType())
                .destinationType(task.getDestinationType())
                .syncConfig(task.getSyncConfig())
                .sourceFieldName(task.getSourceFieldName())
                .targetFieldName(task.getTargetFieldName())
                .fields(task.getFields())
                .build();
    }

    /**
     * 构建统计模型
     */
    private ProcessModel buildStatsModel(SyncStats stats) {
        // 获取任务配置信息（用于任务名称）
        SyncTaskConfig taskConfig = taskConfigCache.getTask(stats.getTaskId());
        String taskName = taskConfig != null ? taskConfig.getTaskName() : stats.getTaskId();

        return ProcessModel.builder()
                .taskId(stats.getTaskId())
                .taskName(taskName)
                .syncStats(stats)
                .build();
    }
}