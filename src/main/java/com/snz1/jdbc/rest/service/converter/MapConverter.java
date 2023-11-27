package com.snz1.jdbc.rest.service.converter;

import java.util.Map;

import com.snz1.utils.JsonUtils;

@SuppressWarnings("unchecked")
public class MapConverter implements org.apache.commons.beanutils.Converter {

  @Override
  public <T> T convert(Class<T> type, Object value) {
    if (value == null) {
      return null;
    }
    return (T)JsonUtils.fromJson(value.toString(), Map.class);
  }

}
