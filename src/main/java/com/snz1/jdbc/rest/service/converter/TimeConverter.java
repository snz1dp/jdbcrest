package com.snz1.jdbc.rest.service.converter;

import java.sql.JDBCType;
import java.sql.Time;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.springframework.stereotype.Component;

@Component("jdbcrest::TimeConverter")
public class TimeConverter extends AbstractConverter {

  public TimeConverter() {
    super(JDBCType.TIME, JDBCType.TIME_WITH_TIMEZONE);
  }

  @Override
  public Object convertObject(Object input) {
    if (input instanceof String) {
      try {
        return new java.sql.Time(new SimpleDateFormat("HH:mm:ss").parse((String)input).getTime());
      } catch(ParseException e1) {
        try {
          return new java.sql.Time(new SimpleDateFormat("HH:mm").parse((String)input).getTime());
        } catch(ParseException e2) {
          throw new IllegalArgumentException(e2.getMessage(), e2);
        }
      }
    } if (input instanceof Date) {
      return new java.sql.Time(((Date)input).getTime());
    } else if (input instanceof Time) {
      return input;
    }  else if (input instanceof java.sql.Date) {
      return new Time(((java.sql.Date)input).getTime());
    } else if (input instanceof Number) {
      return new Time(((Number)input).longValue());
    }
    return input;
  }

}
