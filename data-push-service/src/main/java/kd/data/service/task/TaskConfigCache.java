package kd.data.service.task;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import kd.data.core.model.SyncStats;
import kd.data.service.model.SyncTaskConfig;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 任务配置缓存
 * @author gaozw
 * @date 2025/8/12 16:14
 */
@SuppressWarnings("unused")
@Component
public class TaskConfigCache {


    @Resource(name = "taskCache")
    private Cache<String,SyncTaskConfig> taskCache;


    @Resource(name = "completedTasksCache")
    private Cache<String, SyncStats> completedTasksCache;

    // 新增：分页获取任务方法
    public List<SyncTaskConfig> getTasks(int offset, int limit) {
        return taskCache.asMap().values().stream()
                .skip(offset)
                .limit(limit)
                .collect(Collectors.toList());
    }

    public SyncTaskConfig getTask(String taskId) {
        return taskCache.getIfPresent(taskId);
    }

    public List<SyncTaskConfig> getAllTasks() {
        return new ArrayList<>(taskCache.asMap().values());
    }

    public void addTask(SyncTaskConfig task) {
        taskCache.put(task.getTaskId(), task);
    }


    public CacheStats stats() {
       return taskCache.stats();
    }

    public void removeTask(String taskId) {
        taskCache.asMap().remove(taskId);
    }

    public boolean containsKey(String taskId) {
      return   taskCache.asMap().containsKey(taskId);
    }

    public SyncStats getTaskStats(String taskId) {
        return completedTasksCache.getIfPresent(taskId);
    }

    public void addTaskStats(String taskId,SyncStats syncStats) {
        completedTasksCache.put(taskId, syncStats);
    }

    public void clear(){
        completedTasksCache.cleanUp();
    }
}
