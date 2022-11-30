package com.snz1.jdbc.rest.service.impl;

import javax.annotation.Resource;

import org.springframework.stereotype.Component;

import com.snz1.jdbc.rest.RunConfig;
import com.snz1.jdbc.rest.Version;
import com.snz1.jdbc.rest.service.AppInfoResolver;

@Component
public class AppInfoResolverImpl implements AppInfoResolver {

  @Resource
  private RunConfig runConfig;

  @Override
  public String getAppId() {
    return runConfig.getApplicationCode();
  }

  @Override
  public Version getVersion() {
    return runConfig.getAppVerison();
  }

}
