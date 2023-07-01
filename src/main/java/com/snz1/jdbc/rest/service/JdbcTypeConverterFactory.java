package com.snz1.jdbc.rest.service;

import java.sql.JDBCType;
import java.util.List;

// jdbc类型转换工厂
public interface JdbcTypeConverterFactory {
  
  // 从对象转换为JDBC对象
  Object convertObject(Object input, JDBCType type);

  // 转换为JDBC列表
  Object convertList(List<?> list, JDBCType type);

  // 转换为JDBC数组
  Object convertArray(Object val, JDBCType type);

}

