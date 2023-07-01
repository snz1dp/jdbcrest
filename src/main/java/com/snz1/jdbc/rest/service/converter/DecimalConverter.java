package com.snz1.jdbc.rest.service.converter;

import java.sql.JDBCType;
import java.text.DecimalFormat;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

@Component("jdbcrest::DecimalConverter")
public class DecimalConverter extends AbstractConverter {

  public DecimalConverter() {
    super(JDBCType.FLOAT, JDBCType.REAL, JDBCType.DOUBLE, JDBCType.NUMERIC, JDBCType.DECIMAL);
  }

  @Override
  public Object convertObject(Object source) {
    if (source instanceof String) {
      try {
        return new DecimalFormat().format((String) source);
      } catch(NumberFormatException e) {
        return StringUtils.equals("true", (String)source) ? 1 : 0;
      }
    } else if (source instanceof Double) {
      return (Double)source;
    } else if (source instanceof Number) {
      return ((Number)source).doubleValue();
    } else if (source instanceof Boolean) {
      return ((Boolean)source) ? 1 : 0;
    } else {
      return source;
    }
  }

}
