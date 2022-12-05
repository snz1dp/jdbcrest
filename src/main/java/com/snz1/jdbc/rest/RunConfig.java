package com.snz1.jdbc.rest;

import javax.annotation.Resource;

import gateway.sc.v2.PermissionDefinition;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class RunConfig {

  @Value("${app.default_url:/swagger-ui/index.html}")
	private String defaultTargetUrl;

  @Value("${app.code}")
  private String applicationCode;

  @Value("${app.user.scope:developer}")
  private String defaultUserScope;

  @Autowired(required = false)
  private PermissionDefinition permissionDefinition;

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

  public PermissionDefinition getPermissionDefinition() {
    return permissionDefinition;
  }

  public boolean hasPermissionDefinition() {
    return this.getPermissionDefinition() != null;
  }

}
