package com.snz1.jdbc.rest.service;

import java.sql.JDBCType;

// Jdbc类型转换器
public interface JdbcTypeConverter {

  // 是否支持JDBC类型
  boolean supportType(JDBCType type);

  // 转换对象
  Object convertObject(Object source);

}
