package com.snz1.jdbc.rest.service.converter;

import java.sql.JDBCType;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

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
        return new java.sql.Timestamp(new SimpleDateFormat(JsonUtils.JsonDateFormat).parse((String)input).getTime());
      } catch(ParseException e1) {
        try {
          return new java.sql.Timestamp(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").parse((String)input).getTime());
        } catch(ParseException e2) {
          try {
            return new java.sql.Timestamp(new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyy", Locale.US).parse((String)input).getTime());
          } catch(ParseException e3) {
            throw new IllegalArgumentException(e2.getMessage(), e2);
          }
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
