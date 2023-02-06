package com.snz1.jdbc.rest.service.converter;

import java.sql.JDBCType;

import org.springframework.stereotype.Component;

@Component
public class BooleanConverter extends AbstractConverter {

  public BooleanConverter() {
    super(JDBCType.BIT);
  }

  @Override
  public Object convertObject(Object source) {
    if (source instanceof String) {
      return Boolean.valueOf((String) source);
    } else if (source instanceof Number) {
      return !source.equals(0);
    } else if (source instanceof Boolean) {
      return ((Boolean)source) ? true : false;
    } else {
      return null;
    }
  }

}
