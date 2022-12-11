package com.snz1.jdbc.rest.service;

import java.sql.JDBCType;
import java.util.List;

public interface JdbcTypeConverterFactory {
  
  Object convertObject(Object input, JDBCType type);

  Object convertList(List<?> list, JDBCType type);

  Object convertArray(Object val, JDBCType type);

}

