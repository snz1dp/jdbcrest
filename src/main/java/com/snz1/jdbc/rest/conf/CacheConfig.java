package com.snz1.jdbc.rest.conf;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.snz1.jdbc.rest.service.JdbcRestProvider;
import com.snz1.jdbc.rest.service.impl.CachedJdbcRestProvider;
import com.snz1.jdbc.rest.service.impl.JdbcRestProviderImpl;
import com.snz1.jdbc.rest.stats.CacheStatisticsCollector;

import gateway.sc.v2.config.CacheStatistics;

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

  @Bean
  @ConditionalOnMissingBean(value = CacheStatisticsCollector.class)
  public CacheStatisticsCollector noneCacheStatisticsCollector() {
    return new CacheStatisticsCollector() {

      @Override
      public CacheStatistics getCacheStatistics() {
        CacheStatistics cs = new CacheStatistics();
        cs.count = 0;
        cs.provider = "inmemory";
        cs.type = "none";
        return cs;
      }

    };
  }

}
