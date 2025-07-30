package kd.data.core.coordinator.factory;

import kd.data.core.coordinator.DistributedCoordinator;
import kd.data.core.model.SyncConfig;

/**
 * 协调器工厂
 *
 * @author gaozw
 * @date 2025/7/25 10:35
 */

public interface CoordinatorFactory {


    DistributedCoordinator createCoordinator(SyncConfig config);
}
