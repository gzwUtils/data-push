package kd.data.web.controller;
import kd.data.core.model.SyncStats;
import kd.data.service.exception.TaskException;
import kd.data.service.model.SyncTaskConfig;
import kd.data.service.task.SyncTaskManager;
import kd.data.web.response.ApiResponse;
import kd.data.web.vo.TaskRequest;
import org.springframework.web.bind.annotation.*;

/**
 * 同步
 * @author gaozw
 * @date 2025/7/24 14:37
 */


@RestController
@RequestMapping("/dataPush")
public class SyncController {

    private static final String SUCCESS = "操作成功";

    private final SyncTaskManager syncTaskManager;

    public SyncController(SyncTaskManager syncTool) {
        this.syncTaskManager = syncTool;
    }

    @PostMapping("/start")
    public ApiResponse<String> startSync(@RequestBody TaskRequest request) {
        try {
            // 加载实体类
            Class<?> sourceEntityClass = Class.forName(request.getSourceEntityClassName());
            Class<?> targetEntityClass = Class.forName(request.getTargetEntityClassName());

            // 转换配置
            SyncTaskConfig taskConfig = convertToTaskConfig(request);

            // 启动任务
            syncTaskManager.startTask(taskConfig, sourceEntityClass, targetEntityClass);

            return ApiResponse.success(SUCCESS);
        } catch (ClassNotFoundException e) {
            return ApiResponse.error("实体类未找到: " + e.getMessage());
        } catch (TaskException e) {
            return ApiResponse.error(e.getMessage());
        }
    }

    /**
     * 停止任务
     */
    @GetMapping("/{taskId}/stop")
    public ApiResponse<String> stopTask(@PathVariable String taskId) {
        try {
            syncTaskManager.stopTask(taskId);
            return ApiResponse.success(SUCCESS);
        } catch (TaskException e) {
            return ApiResponse.error(e.getMessage());
        }
    }

    /**
     * 获取任务状态
     */
    @GetMapping("/{taskId}/stats")
    public ApiResponse<SyncStats> getTaskStats(@PathVariable String taskId) {
        try {
            SyncStats stats = syncTaskManager.getTaskStats(taskId);
            return ApiResponse.success(stats);
        } catch (TaskException e) {
            return ApiResponse.error(e.getMessage());
        }
    }

    private SyncTaskConfig convertToTaskConfig(TaskRequest request) {
        SyncTaskConfig config = new SyncTaskConfig();
        config.setTaskId(request.getTaskId());
        config.setTaskName(request.getTaskName());
        config.setSourceType(request.getSourceType());
        config.setSourceConfig(request.getSourceConfig());
        config.setDestinationConfig(request.getDestinationConfig());
        config.setSyncConfig(request.getSyncConfig());
        return config;
    }
}
