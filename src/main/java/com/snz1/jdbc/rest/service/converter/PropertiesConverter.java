package com.snz1.jdbc.rest.service.converter;

import java.util.Properties;

import com.snz1.utils.JsonUtils;

@SuppressWarnings("unchecked")
public class PropertiesConverter implements org.apache.commons.beanutils.Converter {

  @Override
  public <T> T convert(Class<T> type, Object value) {
    if (value == null) {
      return null;
    }
    return (T)JsonUtils.fromJson(value.toString(), Properties.class);
  }

}
