package kd.data.persistence.template;

import kd.data.persistence.KdProcess;
import lombok.Data;

import java.util.List;

/**
 * 持久化模板
 * @author gaozw
 * @date 2025/8/8 13:55
 */
@SuppressWarnings("all")
@Data
public  class PersistenceTemplate {

    private List<KdProcess> kdProcessList;


}
