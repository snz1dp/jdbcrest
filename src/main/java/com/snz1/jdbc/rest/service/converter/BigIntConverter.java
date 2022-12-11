package com.snz1.jdbc.rest.service.converter;

import java.math.BigInteger;
import java.sql.JDBCType;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

@Component
public class BigIntConverter extends AbstractConverter {

  public BigIntConverter() {
    super(JDBCType.BIGINT);
  }

  @Override
  public Object convertObject(Object source) {
    if (source instanceof String) {
      try {
        return new BigInteger((String) source);
      } catch(NumberFormatException e) {
        return StringUtils.equals("true", (String)source) ? 1 : 0;
      }
    } else if (source instanceof BigInteger) {
      return (BigInteger)source;
    } else if (source instanceof Number) {
      return ((Number)source).intValue();
    } else if (source instanceof Boolean) {
      return ((Boolean)source) ? 1 : 0;
    } else {
      return source;
    }
  }

}
