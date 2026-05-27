package com.payments.tbjava;

import com.payments.tbjava.config.EngineConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(EngineConfig.class)
public class TbJavaApplication {

    public static void main(String[] args) {
        SpringApplication.run(TbJavaApplication.class, args);
    }
}
