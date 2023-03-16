package com.snz1.jdbc.rest.service;

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

  // 获取驱动ID
  String getDriverId(String product_name);

}
