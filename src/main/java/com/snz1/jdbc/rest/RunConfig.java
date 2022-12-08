package com.snz1.jdbc.rest;

import javax.annotation.Resource;

import gateway.sc.v2.PermissionDefinition;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class RunConfig {

  @Value("${server.context-path:/jdbc/rest/api}")
  private String webroot;

  @Value("${app.default_url:/swagger-ui/index.html}")
	private String defaultTargetUrl;

  @Value("${app.code}")
  private String applicationCode;

  @Value("${app.sql.location:}")
  private String sql_location;

  @Autowired(required = false)
  private PermissionDefinition permissionDefinition;

  @Resource
  private Version appVerison;

  public String getWebroot() {
    return webroot;
  }

  public String getDefaultTargetUrl() {
    return defaultTargetUrl;
  }

  public String getApplicationCode() {
    return applicationCode;
  }

  public Version getAppVerison() {
    return appVerison;
  }

  public PermissionDefinition getPermissionDefinition() {
    return permissionDefinition;
  }

  public boolean hasPermissionDefinition() {
    return this.getPermissionDefinition() != null;
  }

  public String getSql_location() {
    return sql_location;
  }

}
