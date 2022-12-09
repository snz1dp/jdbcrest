package com.snz1.jdbc.rest.data;

import java.io.Serializable;

import lombok.Data;

// Jdbc转Restful请求
@Data
public abstract class JdbcRestfulRequest implements Serializable, Cloneable {
    
  // 请求ID
  private String request_id;

  // 表名
  private String table_name;

  // 克隆
  public JdbcRestfulRequest clone() {
    try {
      return (JdbcRestfulRequest)super.clone();
    } catch (CloneNotSupportedException e) {
      throw new IllegalStateException(e.getMessage(), e);
    }
  }

}
