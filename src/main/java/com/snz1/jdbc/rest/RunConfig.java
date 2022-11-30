package com.snz1.jdbc.rest;

import javax.annotation.Resource;

import com.snz1.utils.Configurer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class RunConfig {

  @Value("${app.default_url:/index.html}")
	private String defaultTargetUrl;

  @Value("${app.code}")
  private String applicationCode;

  @Value("${app.user.scope:developer}")
  private String defaultUserScope;

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

  /**
   * 获取缺省的用户组织域代码
   * @return
   */
  public String getDefaultUserScope() {
    return Configurer.getAppProperty("default.user_scope", this.defaultUserScope);
  }

  /**
   * 设置缺省的用户组织域代码
   * @param val
   */
  public void getDefaultUserScope(String val) {
    Configurer.setAppProperty("default.user_scope", val);
  }

}
