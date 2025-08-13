package kd.data.persistence.trigger;
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

    // 定时持久化
    @Scheduled(fixedRate = 1000)
    public void scheduledPersist() {
        R result = persistenceServiceHandle();
        if(log.isDebugEnabled()){
            log.debug("定时持久化结果: 成功={}, 数量={}",
                    result.getSuccess(), result.getMessage());
        }
    }

    private R persistenceServiceHandle() {
        List<SyncTaskConfig> allTasks = taskConfigCache.getAllTasks();

        if(allTasks.isEmpty()){
            return R.success();
        }
        try {
            allTasks.forEach(t->{

                ProcessModel processModel = ProcessModel.builder().taskId(t.getTaskId()).taskName(t.getTaskName()).desConfig(t.getDestinationConfig())
                        .sourceConfig(t.getSourceConfig()).sourceType(t.getSourceType()).build();
                ProcessContext context = ProcessContext.builder()
                        .strategyCode(PERSISTENCE.name())
                        .model(processModel)
                        .needBreak(false)
                        .response(R.success()).build();
                ProcessContext process = processControl.process(context);
                if(!process.getNeedBreak()){
                    taskConfigCache.removeTask(processModel.getTaskId());
                }
            });
            return R.success(String.valueOf(allTasks.size()));
        } catch (Exception e){
            log.error("持久化失败 {}",e.getMessage(),e);
            return R.fail(String.valueOf(allTasks.size()));
        }
    }

    // 停机持久化
    @PreDestroy
    public void shutdownPersist() {
        R result = persistenceServiceHandle();
        log.info("停机持久化结果: 成功={}, 数量={}",
                result.getSuccess(), result.getMessage());
    }
}
