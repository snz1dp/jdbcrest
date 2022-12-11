package com.snz1.jdbc.rest.service.converter;

import java.sql.JDBCType;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.springframework.stereotype.Component;

import com.snz1.utils.JsonUtils;

@Component
public class TimestampConverter extends AbstractConverter {

  public TimestampConverter() {
    super(JDBCType.TIMESTAMP, JDBCType.TIMESTAMP_WITH_TIMEZONE);
  }

  @Override
  public Object convertObject(Object input) {
    if (input instanceof String) {
      try {
        return new SimpleDateFormat(JsonUtils.JsonDateFormat).parse((String)input);
      } catch(ParseException e1) {
        try {
          return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").parse((String)input);
        } catch(ParseException e2) {
          throw new IllegalArgumentException(e2.getMessage(), e2);
        }
      }
    } else if (input instanceof Date) {
      return new java.sql.Timestamp(((Date)input).getTime());
    } else if (input instanceof Number) {
      return new Date(((Number)input).longValue());
    }
    return input;
  }

}
