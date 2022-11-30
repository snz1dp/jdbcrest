package com.snz1.jdbc.rest.service;

import com.snz1.jdbc.rest.Version;

// 应用信息获取
public interface AppInfoResolver {

  // 获取应用ID
  String getAppId();

  // 获取版本
  Version getVersion();

}
