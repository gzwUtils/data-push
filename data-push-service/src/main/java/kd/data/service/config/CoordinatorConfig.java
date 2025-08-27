package kd.data.service.config;

import kd.data.core.coordinator.factory.CoordinatorFactory;
import kd.data.core.coordinator.factory.UniversalCoordinatorFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author gaozw
 * @date 2025/7/25 18:14
 */
@Configuration
public class CoordinatorConfig {

    @Bean
    public CoordinatorFactory coordinatorFactory() {
        return new UniversalCoordinatorFactory();
    }
}
