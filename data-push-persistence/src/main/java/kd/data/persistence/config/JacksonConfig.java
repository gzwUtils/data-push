package kd.data.persistence.config;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * 设置序列化工具
 */
@Configuration
public class JacksonConfig {

    private static final String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    @Bean
    public ObjectMapper objectMapper() {  // 推荐使用标准名称
        ObjectMapper objectMapper = new ObjectMapper();

        // 创建JavaTimeModule
        JavaTimeModule javaTimeModule = new JavaTimeModule();

        // 配置LocalDateTime的序列化和反序列化格式
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(DATE_TIME_FORMAT);
        javaTimeModule.addSerializer(LocalDateTime.class,
                new LocalDateTimeSerializer(formatter));
        javaTimeModule.addDeserializer(LocalDateTime.class,
                new LocalDateTimeDeserializer(formatter));

        // 注册模块
        objectMapper.registerModule(javaTimeModule);

        return objectMapper;
    }

}
