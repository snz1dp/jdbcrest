package com.snz1.jdbc.rest.service.opensearch;

import java.sql.SQLException;
import java.util.Map;

public class ObjectTypeConverter implements org.opensearch.jdbc.types.TypeConverter {

  @Override
  @SuppressWarnings("unchecked")
  public <T> T convert(Object value, Class<T> clazz, Map<String, Object> conversionParams) throws SQLException {
    if (value == null) return null;
    if (clazz == null) return (T)value;
    throw new UnsupportedOperationException("Unimplemented method 'convert'");
  }

}
