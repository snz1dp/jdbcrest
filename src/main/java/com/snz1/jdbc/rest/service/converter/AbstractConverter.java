package com.snz1.jdbc.rest.service.converter;

import java.sql.JDBCType;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.snz1.jdbc.rest.service.JdbcTypeConverter;

public abstract class AbstractConverter implements JdbcTypeConverter {
  
  private Set<JDBCType> jdbcTypes = new HashSet<JDBCType>();

  public AbstractConverter(JDBCType...type) {
    this.jdbcTypes.addAll(Arrays.asList(type));
  }

  @Override
  public boolean supportType(JDBCType type) {
    return this.jdbcTypes.contains(type);
  }

  @Override
  public Object convertObject(Object source) {
    if (source == null) return null;
    return doConvertObject(source);
  }

  protected Object doConvertObject(Object source) {
    throw new UnsupportedOperationException();
  }

}
