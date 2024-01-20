package com.snz1.jdbc.rest.service.impl;

import java.lang.reflect.Array;

import org.apache.commons.beanutils.Converter;

import com.snz1.utils.JsonUtils;

public class ConvertUtilsBean extends org.apache.commons.beanutils.ConvertUtilsBean {

  public String convert(Object value) {

    if (value == null) {
      return null;
    } else if (value.getClass().isArray()) {
      if (Array.getLength(value) < 1) {
        return (null);
      }
      return JsonUtils.toJson(value);
    } else {
      final Converter converter = lookup(String.class);
      return (converter.convert(String.class, value));
    }

  }

}