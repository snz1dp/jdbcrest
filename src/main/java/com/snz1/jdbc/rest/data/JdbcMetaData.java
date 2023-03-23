package com.snz1.jdbc.rest.data;

import java.io.Serializable;

import lombok.Data;

@Data
public class JdbcMetaData implements Serializable {

  // 驱动ID
  private String driver_id;
  
  // 提供器类
  private String provider_class;
    
  // 数据库类型
  private String product_name;

  // 数据库版本
  private String product_version;

  // 驱动名称
  private String driver_name;

  // 驱动类
  private String driver_class;

  // 驱动版本
  private String driver_version;

  // 连接URL
  private String jdbc_url;

  // 连接用户
  private String jdbc_user;

  // 是否启用单点
  private Boolean sso_enabled;

  // 是否支持Schema
  private Boolean support_schema;

  // 是否启用预定义配置
  private Boolean predefined_enabled;

  // 动态配置类型
  private String dynamic_config_type = "none";

  // 缓存类型
  private String cache_type = "none";

}
