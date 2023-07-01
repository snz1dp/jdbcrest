package com.snz1.jdbc.rest.service.converter;

import java.sql.JDBCType;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.springframework.stereotype.Component;

@Component("jdbcrest::DateConverter")
public class DateConverter extends AbstractConverter {

  public DateConverter() {
    super(JDBCType.DATE);
  }

  @Override
  public Object convertObject(Object input) {
    if (input instanceof String) {
      try {
        return new java.sql.Date(new SimpleDateFormat("yyyy-MM-dd").parse((String)input).getTime());
      } catch(ParseException e) {
        throw new IllegalArgumentException(e.getMessage(), e);
      }
    } else if (input instanceof java.sql.Date) {
      return input;
    } else if (input instanceof Date) {
      return new java.sql.Date(((Date)input).getTime());
    } else if (input instanceof Number) {
      return new java.sql.Date(((Number)input).longValue());
    }
    return input;
  }

}
