package com.snz1.jdbc.rest;

import java.io.Serializable;
import java.text.MessageFormat;

import org.springframework.beans.factory.annotation.Value;

public class Version implements Serializable {

  private static final long serialVersionUID = -660884757648964031L;

  // 产品主版本号
  public static final int MAJOR = 1;

  // 产品子版本号
  public static final int MINOR = 0;

  // 产品修订版本号
  public static final int REVSION = 3;

  // 产品编译版本号
  public static final int BUILD = 919;

  // 产品版本字符串
  public static final String VERSION = String.format("%d.%d.%d-%d", MAJOR, MINOR, REVSION, BUILD);

  // 产品代码
  @Value("${app.code:jdbcrest}")
  private String product_code = "jdbcrest";

  // 产品名称
  @Value("${app.title:Jdbc\u8F6CRest\u670D\u52A1}")
  private String product_name = "Jdbc转Rest服务";

  // 公司名称
  @Value("${app.company.name:Jdbc\u8F6CRest\u670D\u52A1}")
  private String company_name = "长沙慧码至一信息科技有限公司";

  // 公司网站
  @Value("${app.company.url:https://snz1.cn}")
  private String company_url;

  // 联系地址
  @Value("${app.contact.email:snz1@qq.com}")
  private String contact_email;

  // 产品描述
  @Value("${app.description:Jdbc\u8F6CRest\u670D\u52A1}")
  public String description;

  // 版权
  @Value("${app.legal.copyright:2023\u00A9\u6167\u7801\u81F3\u4E00}")
  public String legal_copyright;

  public String getProduct_code() {
    return product_code;
  }

  public String getProduct_name() {
    return product_name;
  }

  public void setProduct_name(String val) {
    product_name = val;
  }

  public String getCompany_name() {
    return company_name;
  }

  public String getCompany_url() {
    return company_url;
  }

  public String getContact_email() {
    return contact_email;
  }

  public String getDescription() {
    return description;
  }

  public String getLegal_copyright() {
    return legal_copyright + getCompany_name();
  }

  public String getProduct_title_with_version() {
    return MessageFormat.format("{0} V{1}.{2}-{3}", product_name, MAJOR, MINOR, REVSION, BUILD);
  }

  public String getProduct_version() {
    return VERSION;
  }

}
