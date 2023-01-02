package com.snz1.jdbc.rest.service.impl;

import javax.annotation.Resource;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.snz1.jdbc.rest.Constants;
import com.snz1.jdbc.rest.RunConfig;
import com.snz1.jdbc.rest.Version;
import com.snz1.jdbc.rest.service.AppInfoResolver;
import com.snz1.utils.Configurer;

import gateway.api.NotExceptException;
import gateway.sc.v2.ToolProvider;
import gateway.sc.v2.config.LicenseSupport;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class AppInfoResolverImpl implements AppInfoResolver {

  @Resource
  private RunConfig runConfig;

  @Resource
  private ToolProvider toolProvider;

  private String lastLicense;

  private LicenseSupport licenseSupport;

  @Override
  public String getAppId() {
    return runConfig.getApplicationCode();
  }

  @Override
  public Version getVersion() {
    return runConfig.getAppVerison();
  }

  public boolean hasLicense() {
    return this.getLicense() != null;
  }

  @Override
  public LicenseSupport getLicense() {
    String lic = null;
    if (runConfig.isPersistenceConfig()) {
      lic = Configurer.getAppProperty(Constants.LICENSE_CODE_ARG, runConfig.getLicense_code());
    } else {
      lic = runConfig.getLicense_code();
    }
    if (StringUtils.equals(lic, this.lastLicense)) {
      return this.licenseSupport;
    }
    try {
      this.licenseSupport = toolProvider.decodeLicense(lic);
      this.lastLicense = lic;
      return this.licenseSupport;
    } catch(NotExceptException e) {
      log.info("授权失败：" + e.getMessage(), e);
    }
    return null;
  }

}
