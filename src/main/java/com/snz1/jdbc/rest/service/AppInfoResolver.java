package com.snz1.jdbc.rest.service;

import com.snz1.jdbc.rest.Version;

import gateway.sc.v2.config.LicenseSupport;

// 应用信息获取
public interface AppInfoResolver {

  // 获取应用ID
  String getAppId();

  // 获取版本
  Version getVersion();

  // 获取授权
  LicenseSupport getLicense();

  // 是否有授权
  boolean hasLicense();

}
