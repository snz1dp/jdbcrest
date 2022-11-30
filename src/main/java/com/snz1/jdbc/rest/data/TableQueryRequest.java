package com.snz1.jdbc.rest.data;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

// 查询描述
@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class TableQueryRequest extends JdbcQueryRequest {

  // 查询表名
  private String table_name;

}
