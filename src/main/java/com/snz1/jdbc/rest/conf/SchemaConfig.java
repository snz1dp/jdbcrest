package com.snz1.jdbc.rest.conf;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration("jdbcrest::SchemaConfig")
@ConditionalOnProperty(prefix = "spring.datascheme", name = "enabled", havingValue = "true", matchIfMissing = false)
@com.snz1.annotation.EnableAutoScheme //启用自动构建数据库
public class SchemaConfig {
  
  public SchemaConfig() {
    log.info("启用数据结构自动构建配置...");
  }

}
