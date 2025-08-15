package kd.data.persistence.action.jdbc;
import kd.data.core.persistence.PersistenceService;
import kd.data.persistence.ProcessModel;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * jdbc
 *
 * @author gaozw
 * @date 2025/8/11 17:11
 */
@Slf4j
public class JdbcPersistenceService implements PersistenceService<ProcessModel> {


    @Override
    public void persist(ProcessModel model) {
            //后续补充
    }

    @Override
    public void persistBatch(List<ProcessModel> models) {
            //后续补充
    }

    @Override
    public void shutdown() {
        //后面处理
    }
}
