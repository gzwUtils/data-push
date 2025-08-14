package kd.data.persistence.trigger;

import com.github.benmanes.caffeine.cache.stats.CacheStats;
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
import java.util.List;
import java.util.stream.Collectors;

import static kd.data.persistence.enums.BusinessStrategyCode.PERSISTENCE;

/**
 * 触发
 *
 * @author gaozw
 * @date 2025/8/12 16:19
 */
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
        int failureCount = 0; // 失败计数器
        final int MAX_FAILURES = 3; // 最大允许失败次数
        try {
            while (failureCount < MAX_FAILURES) {
                List<SyncTaskConfig> batch = taskConfigCache.getTasks(page, pageSize);

                batch = batch.stream().filter(t->!t.isPersist()).collect(Collectors.toList());

                if (batch.isEmpty()) {
                    break;
                }
                for (SyncTaskConfig task : batch) {
                    if (failureCount >= MAX_FAILURES) {
                        break;
                    }
                    if (processTask(task)) {
                        processed++;
                        // 成功处理后立即移除
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

    // 停机持久化
    @PreDestroy
    public void shutdownPersist() {
        List<SyncTaskConfig> allTasks = taskConfigCache.getAllTasks();
        int successCount = 0;

        for (SyncTaskConfig task : allTasks) {
            if (processTask(task)) {
                successCount++;
                task.setPersist(true);
                taskConfigCache.addTask(task);
            }
        }

        log.info("停机处理完成: 总数={}, 成功={}", allTasks.size(), successCount);
    }

    // 添加缓存统计
    @Scheduled(fixedRate = 1800000)
    public void logCacheStats() {
        CacheStats stats = taskConfigCache.stats();
        log.info("缓存统计: 命中率={}%, 淘汰数={}, 加载数={}",
                stats.hitRate() * 100,
                stats.evictionCount(),
                stats.loadCount());
    }


    // 独立任务处理方法
    private boolean processTask(SyncTaskConfig task) {
        try {
            ProcessModel model = ProcessModel.builder()
                    .taskId(task.getTaskId())
                    .taskName(task.getTaskName())
                    .desConfig(task.getDestinationConfig())
                    .sourceConfig(task.getSourceConfig())
                    .sourceType(task.getSourceType())
                    .build();

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

}
