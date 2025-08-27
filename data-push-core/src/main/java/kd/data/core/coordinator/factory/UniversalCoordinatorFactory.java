package kd.data.core.coordinator.factory;

import kd.data.core.coordinator.DistributedCoordinator;
import kd.data.core.coordinator.InMemoryCoordinator;
import kd.data.core.coordinator.RedisCoordinator;
import kd.data.core.model.SyncConfig;

import java.util.Map;

/**
 * 通用协调器工厂
 * @author redis
 * @date 2025/7/25 11:26
 */
@SuppressWarnings("unused")
public class UniversalCoordinatorFactory implements CoordinatorFactory {

    @Override
    public DistributedCoordinator createCoordinator(SyncConfig config) {
        Map<String, String> coordinatorConfig = config.getCoordinatorConfig();
        String type = config.getCoordinatorType();
        switch (type.toLowerCase()) {
            case "redis":
                String host = coordinatorConfig.getOrDefault("host", "localhost");
                int port = Integer.parseInt(coordinatorConfig.getOrDefault("port", "6379"));
                String password = coordinatorConfig.get("password");
                return new RedisCoordinator(host, port, password);
            case "memory":
            default:
                return new InMemoryCoordinator();
        }
    }
}
