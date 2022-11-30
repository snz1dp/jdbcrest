package com.snz1.jdbc.rest.data;

import java.io.Serializable;

import lombok.Data;

@Data
public class JdbcMetaData implements Serializable {
    
  // 数据库类型
  private String product_name;

  // 数据库版本
  private String product_version;

  // 驱动名称
  private String driver_name;

  // 驱动版本
  private String driver_version;

}
