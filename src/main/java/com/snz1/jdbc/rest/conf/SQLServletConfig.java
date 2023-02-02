package com.snz1.jdbc.rest.conf;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.snz1.jdbc.rest.RunConfig;
import com.snz1.jdbc.rest.servlet.SQLServiceCacheDeleteServlet;
import com.snz1.jdbc.rest.servlet.SQLServiceRequestExecuteServlet;

@Configuration
public class SQLServletConfig {

  @Bean
  @ConditionalOnMissingBean(RunConfig.class)
  public RunConfig runConfig() {
    return new RunConfig();
  }

  @Bean
  public SQLServiceRequestExecuteServlet sqlServiceRequestExecuteServlet() {
    return new SQLServiceRequestExecuteServlet();
  }

  @Bean
  public ServletRegistrationBean<SQLServiceRequestExecuteServlet> sqlServiceRequestExecuteRegistrationBean(SQLServiceRequestExecuteServlet servlet) {
    return new ServletRegistrationBean<SQLServiceRequestExecuteServlet>(
      servlet, SQLServiceRequestExecuteServlet.PATH
    );
  }

  @Bean
  public SQLServiceCacheDeleteServlet sqlServiceCacheDeleteServlet() {
    return new SQLServiceCacheDeleteServlet();
  }

  @Bean
  public ServletRegistrationBean<SQLServiceCacheDeleteServlet> sqlServiceCacheDeleteServletRegistrationBean(SQLServiceCacheDeleteServlet servlet) {
    return new ServletRegistrationBean<SQLServiceCacheDeleteServlet>(
      servlet, SQLServiceCacheDeleteServlet.PATH
    );
  }

}
