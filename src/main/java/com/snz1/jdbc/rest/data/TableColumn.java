package com.snz1.jdbc.rest.data;

import java.io.Serializable;
import java.sql.JDBCType;

import lombok.Data;

@Data
public class TableColumn implements Serializable{
  
  // 序号
  private Integer index;

  // 字段名称
  private String name;

  // 字段标签
  private String label;

  // 类型
  private String sql_type;

  // JdbcType
  private JDBCType jdbc_type;

  // 显示宽度
  private Integer display_size;

  // 精度
  private Integer precision;

  // 刻度
  private Integer scale;

  // 是否只读
  private Boolean read_only;

  // 是否可写
  private Boolean writable;

  // 是否自动增长
  private Boolean auto_increment;

  // 是否可用于查询条件
  private Boolean searchable;

  // 是否货币类型字段
  private Boolean currency;

  // 是否可空
  private Boolean nullable;

  // 是否区分大小写
  private Boolean case_sensitive;

  // 表名
  private String table_name;

}
