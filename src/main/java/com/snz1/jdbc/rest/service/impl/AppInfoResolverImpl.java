package com.snz1.jdbc.rest.service.impl;

import java.util.Calendar;
import java.util.Date;

import javax.annotation.Resource;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.springframework.stereotype.Component;

import com.snz1.jdbc.rest.Constants;
import com.snz1.jdbc.rest.RunConfig;
import com.snz1.jdbc.rest.Version;
import com.snz1.jdbc.rest.data.LicenseMeta;
import com.snz1.jdbc.rest.service.AppInfoResolver;
import com.snz1.utils.CalendarUtils;
import com.snz1.utils.Configurer;
import com.snz1.utils.TimeZoneUtils;

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
    return this.getLicenseSupport() != null;
  }

  @Override
  public LicenseSupport getLicenseSupport() {
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

  @Override
  public LicenseMeta getLicenseMeta() {
    Date first_run_time  = runConfig.getFirstRunTime();
    LicenseSupport license_support = this.getLicenseSupport();
    LicenseMeta ret = null;
    if (license_support != null) {
      if (license_support.getPrebationary() != null) {
        Date end_time = CalendarUtils.add(first_run_time, TimeZoneUtils.getCurrent(), Calendar.DATE, license_support.getPrebationary());
        ret = new LicenseMeta(end_time, "企业订阅版", license_support.getCustomer(), license_support.getProvider(), license_support.getSignature());
      } else {
        Date end_time = null;
        ret = new LicenseMeta(end_time, "高级企业版", license_support.getCustomer(), license_support.getProvider(), license_support.getSignature());
      }
    } else {
      Date end_time = CalendarUtils.add(first_run_time, TimeZoneUtils.getCurrent(), Calendar.MONTH, 3);
      ret = new LicenseMeta(end_time, "临时体验版", null, null, null);
    }
    if (ret != null && ret.getEnd() != null) {
      Validate.isTrue(
        new Date().before(ret.getEnd()),
        "%s授权已到期, 请购买授权后重试。",
        ret.getHint()
      );
    }
    return ret;
  }

}
