package com.snz1.jdbc.rest;

import java.io.Serializable;
import java.text.MessageFormat;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class Version implements Serializable {

  private static final long serialVersionUID = -660884757648964031L;

  // 产品主版本号
  public static final int MAJOR = 1;

  // 产品子版本号
  public static final int MINOR = 0;

  // 产品修订版本号
  public static final int REVSION = 1;

  // 产品编译版本号
  public static final int BUILD = 312;

  // 产品版本字符串
  public static final String VERSION = String.format("%d.%d.%d-%d", MAJOR, MINOR, REVSION, BUILD);

  // 产品代码
  @Value("${app.code:jdbcrest}")
  private String PRODUCT_CODE = "jdbcrest";

  // 产品名称
  private String PRODUCT_NAME = "Jdbc转Rest服务";

  // 公司名称
  private String COMPANY_NAME = "长沙慧码至一信息科技有限公司";

  // 公司网站
  private String COMPANY_URL = "https://snz1.cn";

  // 联系地址
  private String CONTACT_EMAIL = "snz1@qq.com";

  // 产品描述
  public static final String DESCRIPTION = "Jdbc转Rest服务";

  // 版权
  public static final String LEGAL_COPYRIGHT = "2023©慧码至一";

  // 版权声明
  public static final String PRODUCT_LICENSE = "长沙慧码至一信息科技有限公司";

  public String getProduct_code() {
    return PRODUCT_CODE;
  }

  public String getProduct_name() {
    return PRODUCT_NAME;
  }

  public void setProduct_name(String val) {
    PRODUCT_NAME = val;
  }

  public String getCompany_name() {
    return COMPANY_NAME;
  }

  public String getCompany_url() {
    return COMPANY_URL;
  }

  public String getContact_email() {
    return CONTACT_EMAIL;
  }

  public String getDescription() {
    return DESCRIPTION;
  }

  public String getLegal_copyright() {
    return LEGAL_COPYRIGHT + getCompany_name();
  }

  public String getProduct_license() {
    return PRODUCT_LICENSE;
  }

  public String getProduct_title_with_version() {
    return MessageFormat.format("{0} V{1}.{2}", PRODUCT_NAME, MAJOR, MINOR, REVSION, BUILD);
  }

  public String getProduct_version() {
    return VERSION;
  }

}
