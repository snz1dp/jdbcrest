package com.snz1.jdbc.rest.conf;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan({
  "com.snz1.jdbc.rest.service",
  "com.snz1.jdbc.rest.servlet",
  "com.snz1.jdbc.rest.conf",
  "com.snz1.jdbc.rest.api",
})
@ConditionalOnMissingBean(com.snz1.jdbc.rest.Version.class)
public class JdbcRestConfig {

  @Bean
  public com.snz1.jdbc.rest.Version jdbcRestVersion() {
    return new com.snz1.jdbc.rest.Version();
  }

  @Bean
  public com.snz1.jdbc.rest.RunConfig jdbcRestRunConfig() {
    return new com.snz1.jdbc.rest.RunConfig();
  }

}
