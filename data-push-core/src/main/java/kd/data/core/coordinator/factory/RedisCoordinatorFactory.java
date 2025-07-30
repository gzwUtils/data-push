package kd.data.core.coordinator.factory;

import kd.data.core.coordinator.DistributedCoordinator;
import kd.data.core.coordinator.RedisCoordinator;
import kd.data.core.model.SyncConfig;

import java.util.Map;

/**
 * @author redis
 * @date 2025/7/25 11:26
 */
@SuppressWarnings("unused")
public class RedisCoordinatorFactory implements CoordinatorFactory {

    @Override
    public DistributedCoordinator createCoordinator(SyncConfig config) {
        Map<String, String> coordinatorConfig = config.getCoordinatorConfig();
        return new RedisCoordinator(coordinatorConfig.getOrDefault("host","localhost"),Integer.parseInt(coordinatorConfig.getOrDefault("port","6379")));
    }
}
