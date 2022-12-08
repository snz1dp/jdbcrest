package com.snz1.jdbc.rest.conf;

import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.snz1.jdbc.rest.servlet.SQLServiceServlet;

@Configuration
public class SQLServletConfig {

  @Bean
  public SQLServiceServlet sqlServiceServlet() {
    return new SQLServiceServlet();
  }

  @Bean
  public ServletRegistrationBean<SQLServiceServlet> servletRegistrationBean(SQLServiceServlet sqlServiceServlet) {
    return new ServletRegistrationBean<SQLServiceServlet>(
      sqlServiceServlet, SQLServiceServlet.PATH
    );
  }

}
