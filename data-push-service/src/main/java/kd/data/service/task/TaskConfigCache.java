package kd.data.service.task;

import kd.data.service.model.SyncTaskConfig;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 任务配置缓存
 * @author gaozw
 * @date 2025/8/12 16:14
 */
@SuppressWarnings("unused")
@Component
public class TaskConfigCache {

    // 内存存储任务配置
    private final Map<String, SyncTaskConfig> taskCache = new ConcurrentHashMap<>();

    public void addTask(SyncTaskConfig task) {
        taskCache.put(task.getTaskId(), task);
    }

    public SyncTaskConfig getTask(String taskId) {
        return taskCache.get(taskId);
    }

    public List<SyncTaskConfig> getAllTasks() {
        return new ArrayList<>(taskCache.values());
    }

    public void removeTask(String taskId) {
        taskCache.remove(taskId);
    }
}
