package com.snz1.jdbc.rest.service;

import java.util.Date;

import com.snz1.jdbc.rest.Version;
import com.snz1.jdbc.rest.data.LicenseMeta;

import gateway.sc.v2.config.LicenseSupport;

// 应用信息获取
public interface AppInfoResolver {

  // 获取应用ID
  String getAppId();

  // 获取版本
  Version getVersion();

  // 获取授权
  LicenseSupport getLicenseSupport();

  // 授权细节
  LicenseMeta getLicenseMeta();

  // 是否有授权
  boolean hasLicense();

  // 获取部署ID
  String getDeploymentId();

  // 是否全局只读
  boolean isGlobalReadonly();

  // 严格模式
  boolean isStrictMode();

  // JDBC URL
  String getJdbcURL();

  // JDBC 用户
  String getJdbcUser();

  // 是否启用了单点登录
  boolean isSsoEnabled();
  
  // 是否启用了预定义配置
  boolean isPredefinedEnabled();
  
  // 获取动态配置类型
  String getDynamiConfigType();

  // 获取缓存类型
  String getCacheType();

  // 获取数据版本
  Integer getSchemaVersion();

  // 支持团队
  String getSupportGroup();

  // 支持EMail
  String getSupportEmail();

  // 支持用户
  String getSupportUsername();

  // 获取开始运行时间
  Date getFirstRunTime();

  // 获取缺省的目标地址
  String getDefaultTargetURL();

}
