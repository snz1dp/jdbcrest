package com.snz1.jdbc.rest.conf;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.snz1.jdbc.rest.service.JdbcRestProvider;
import com.snz1.jdbc.rest.service.impl.CachedJdbcRestProvider;
import com.snz1.jdbc.rest.service.impl.JdbcRestProviderImpl;

@Configuration
public class CacheConfig {
  
  @Bean
  @ConditionalOnProperty(prefix = "spring.cache", name = "type", havingValue = "none", matchIfMissing = false)
  public JdbcRestProviderImpl defaultJdbcRestProvider() {
    return new JdbcRestProviderImpl();
  }

  @Bean
  @ConditionalOnMissingBean(JdbcRestProvider.class)
  public CachedJdbcRestProvider cachedJdbcRestProvider() {
    return new CachedJdbcRestProvider();
  }

}
