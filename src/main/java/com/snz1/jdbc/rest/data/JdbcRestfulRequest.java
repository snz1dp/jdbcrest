package com.snz1.jdbc.rest.data;

import java.io.Serializable;
import java.util.Date;
import lombok.Data;

// Jdbc转Restful请求
@Data
public abstract class JdbcRestfulRequest implements Serializable, Cloneable {

  // 表名
  private String table_name;

  // 请求ID
  private String request_id;

  // 定义
  private TableDefinition definition;

  // 更新时间
  private Date request_time = new Date();

  // 是否有定义
  public boolean hasDefinition() {
    return this.definition != null;
  }

  // 克隆
  public JdbcRestfulRequest clone() {
    try {
      return (JdbcRestfulRequest)super.clone();
    } catch (CloneNotSupportedException e) {
      throw new IllegalStateException(e.getMessage(), e);
    }
  }

}
