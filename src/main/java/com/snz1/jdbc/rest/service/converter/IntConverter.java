package com.snz1.jdbc.rest.service.converter;

import java.sql.JDBCType;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

@Component("jdbcrest::IntConverter")
public class IntConverter extends AbstractConverter {

  public IntConverter() {
    super(JDBCType.TINYINT, JDBCType.SMALLINT, JDBCType.INTEGER);
  }

  @Override
  public Object convertObject(Object source) {
    if (source instanceof String) {
      try {
        return Integer.valueOf((String) source);
      } catch(NumberFormatException e) {
        return StringUtils.equals("true", (String)source) ? 1 : 0;
      }
    } else if (source instanceof Number) {
      return ((Number)source).intValue();
    } else if (source instanceof Boolean) {
      return ((Boolean)source) ? 1 : 0;
    } else {
      return source;
    }
  }

}
