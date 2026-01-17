package kd.data.persistence.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import kd.data.core.persistence.PersistenceService;
import kd.data.persistence.ProcessControl;
import kd.data.persistence.ProcessModel;
import kd.data.persistence.action.file.FilePersistenceService;
import kd.data.persistence.action.jdbc.JdbcPersistenceService;
import kd.data.persistence.enums.BusinessStrategyCode;
import kd.data.persistence.strategy.PersistenceHandler;
import kd.data.persistence.strategy.PreParamCheckHandler;
import kd.data.persistence.template.PersistenceTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author gaozw
 * @date 2025/8/11 18:31
 */
@SuppressWarnings("unused")
@EnableScheduling
@ConditionalOnProperty(prefix = "persistence", name = "enabled", havingValue = "true")
@Configuration
public class PipelineConfig {


    @Value("${persistence.mode:jdbc}")
    private  String mode;

    @Resource
    private ObjectMapper objectMapper;

    @Bean
    public PersistenceService<ProcessModel> persistenceService() {
        switch (mode.toLowerCase()) {
            case "jdbc":
                return new JdbcPersistenceService();
            case "file":
                return new FilePersistenceService(objectMapper);
            default:
                throw new IllegalArgumentException("不支持的持久化模式: " + mode);
        }
    }

    /**
     * 持久化执行流程
     * 1.校验参数
     * 2.持久化
     *
     * @return 执行结果
     */
    @Bean("persistenceTemplate")
    public PersistenceTemplate persistenceTemplate() {
        PersistenceTemplate processTemplate = new PersistenceTemplate();
        processTemplate.setKdProcessList(Arrays.asList(new PreParamCheckHandler(), new PersistenceHandler(persistenceService())));
        return processTemplate;
    }

    /**
     * pipeline流程控制器
     * @return 初始化定义
     */
    @Bean
    public ProcessControl processController() {
        ProcessControl processControl = new ProcessControl();
        Map<String, PersistenceTemplate> templateConfig = new HashMap<>(4);
        templateConfig.put(BusinessStrategyCode.PERSISTENCE.name(), persistenceTemplate());
        processControl.setTemplateConfig(templateConfig);
        return processControl;
    }
}
