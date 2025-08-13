package kd.data.persistence.strategy;

import kd.data.core.persistence.PersistenceService;
import kd.data.persistence.KdProcess;
import kd.data.persistence.ProcessContext;
import kd.data.persistence.ProcessModel;
import lombok.extern.slf4j.Slf4j;

/**
 * 持久化策略
 *
 * @author gaozw
 * @date 2025/8/11 18:05
 */
@SuppressWarnings("unused")
@Slf4j
public class PersistenceHandler implements KdProcess<ProcessModel> {

    private final PersistenceService<ProcessModel> persistenceService;

    public PersistenceHandler(PersistenceService<ProcessModel> persistenceService) {
        this.persistenceService = persistenceService;
    }

    @Override
    public void process(ProcessContext<ProcessModel> content) {
        log.info("start execute persistence .......");
        persistenceService.persist(content.getModel());
    }
}
