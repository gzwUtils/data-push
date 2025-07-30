package kd.data.web;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;


@SpringBootApplication
@ComponentScan(basePackages = {"kd.data.core", "kd.data.service", "kd.data.web"})
public class SYncPushApplication {
    public static void main(String[] args) {
        SpringApplication.run(SYncPushApplication.class,args);
    }
}