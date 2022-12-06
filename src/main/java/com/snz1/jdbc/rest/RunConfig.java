package com.snz1.jdbc.rest;

import javax.annotation.Resource;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class RunConfig {

  @Value("${app.default_url:/index.html}")
	private String defaultTargetUrl;

  @Value("${app.code}")
  private String applicationCode;

  @Value("${app.sql.location:}")
  private String sql_location;

  @Resource
  private Version appVerison;

  public String getDefaultTargetUrl() {
    return defaultTargetUrl;
  }

  public String getApplicationCode() {
    return applicationCode;
  }

  public Version getAppVerison() {
    return appVerison;
  }

  public String getSql_location() {
    return sql_location;
  }

}
