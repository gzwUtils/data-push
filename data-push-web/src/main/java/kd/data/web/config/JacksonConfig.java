package kd.data.web.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 设置序列化工具
 */
@Configuration
public class JacksonConfig {

    @Bean
    public ObjectMapper objectMapper() {  // 推荐使用标准名称
        ObjectMapper mapper = new ObjectMapper();

        // 允许反序列化时忽略未知字段
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        // 允许序列化空 Bean
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

        // 【可选】支持 Java 8 时间类型
        mapper.registerModule(new JavaTimeModule());
        // 避免 LocalDateTime 被序列化为数组
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        return mapper;
    }

}
