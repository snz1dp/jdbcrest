package com.snz1.jdbc.rest.data;

import java.io.Serializable;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

// 查询SQL与参数声明
@Data
@AllArgsConstructor
@NoArgsConstructor
public class JdbcQueryStatement implements Serializable {

  // 查询语句
  private String sql;

  // 参数列表
  private List<Object> parameters;

  public boolean hasParameter() {
    return this.parameters != null && this.parameters.size() > 0;
  }
  
}
