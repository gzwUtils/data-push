package kd.data.service.task;

import kd.data.core.coordinator.factory.CoordinatorFactory;
import kd.data.core.coordinator.DistributedCoordinator;
import kd.data.core.core.BigDataSyncTool;
import kd.data.core.customer.BatchConsumerService;
import kd.data.core.data.DataAccessor;
import kd.data.core.model.SyncStats;
import kd.data.service.datasource.ConsumerDataAccessFactoryManager;
import kd.data.service.datasource.DataAccessorFactoryManager;
import kd.data.service.datasource.DataSourceManager;
import kd.data.service.exception.TaskException;
import kd.data.service.model.SyncTaskConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import javax.annotation.Resource;
import javax.sql.DataSource;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author gaozw
 * @date 2025/7/25 10:44
 */
@SuppressWarnings({"unused"})
@Slf4j
@Service
public class SyncTaskManager {

    private final Map<String, BigDataSyncTool<?>> runningTasks = new ConcurrentHashMap<>();
    @Resource
    private DataAccessorFactoryManager accessorFactoryManager;
    @Resource
    private CoordinatorFactory coordinatorFactory;

    @Resource
    private ConsumerDataAccessFactoryManager consumerDataAccessFactoryManager;



    public void startTask(SyncTaskConfig config,
                          Class<?> sourceEntityClass,
                          Class<?> targetEntityClass) {

        doStartTask(config,
                captureGeneric(sourceEntityClass),
                captureGeneric(targetEntityClass));
    }

    /**
     * @param config 任务配置
     * @param sourceEntityClass 源实体类（用于数据访问）
     * @param targetEntityClass 目标实体类（用于消费者）
     */
    public <T> void doStartTask(SyncTaskConfig config, Class<T> sourceEntityClass,
                          Class<T> targetEntityClass) {

        String taskId = config.getTaskId();
        if (runningTasks.containsKey(taskId)) {
            throw new TaskException("任务已在运行: " + taskId);
        }
        try {
            // 1. 创建源数据源
            DataSource sourceDataSource = DataSourceManager.getJdbcDataSource(
                    config.getSourceConfig());


            // 2. 创建数据访问器
            DataAccessor<T> dataAccessor = accessorFactoryManager.createDataAccessor(
                    sourceEntityClass,
                    sourceDataSource,
                    config.getSyncConfig()
            );

            // 3. 创建分布式协调器
            DistributedCoordinator coordinator = coordinatorFactory.createCoordinator(
                    config.getSyncConfig());

            // 4. 创建消费者
            BatchConsumerService<T> batchConsumer = consumerDataAccessFactoryManager.createConsumer(
                    targetEntityClass,
                    config.getDestinationConfig()
            );

            // 5. 创建并启动同步引擎
            BigDataSyncTool<T> syncTool = new BigDataSyncTool<>(
                    config.getSyncConfig(),
                     dataAccessor,
                     batchConsumer,
                    coordinator
            );

            // 6. 启动同步任务
            new Thread(syncTool::startSync).start();
            runningTasks.put(config.getTaskId(), syncTool);
        } catch (Exception e) {
            log.error("Failed to start task: {}", config.getTaskId(), e);
            throw new TaskException("Task start failed", e);
        }
    }

    /**
     * 停止任务
     * @param taskId 任务ID
     */
    public void stopTask(String taskId) {
        BigDataSyncTool<?> syncTool = runningTasks.get(taskId);
        if (syncTool != null) {
            syncTool.stopSync();
            runningTasks.remove(taskId);
        } else {
            throw new TaskException("任务未运行: " + taskId);
        }
    }

    /**
     * 获取任务状态
     * @param taskId 任务ID
     * @return 同步统计信息
     */
    public SyncStats getTaskStats(String taskId) {
        BigDataSyncTool<?> syncTool = runningTasks.get(taskId);
        if (syncTool == null) {
            throw new TaskException("任务未运行: " + taskId);
        }
        return syncTool.getStats();
    }


    @SuppressWarnings("unchecked")
    private static <T> Class<T> captureGeneric(Class<?> clazz) {
        return (Class<T>) clazz;
    }
}
