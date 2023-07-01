package com.snz1.jdbc.rest.service.converter;

import java.sql.JDBCType;
import org.springframework.stereotype.Component;

@Component("jdbcrest::StringConverter")
public class StringConverter extends AbstractConverter {

  public StringConverter() {
    super(JDBCType.CHAR, JDBCType.VARCHAR, JDBCType.LONGVARCHAR, JDBCType.NVARCHAR, JDBCType.LONGNVARCHAR);
  }

  @Override
  public Object convertObject(Object input) {
    return input instanceof String ? input : input + "";
  }

}
