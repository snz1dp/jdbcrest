package com.snz1.jdbc.rest.utils;

import java.lang.reflect.InvocationTargetException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.beanutils.BeanUtilsBean;

public abstract class ObjectUtils extends org.apache.commons.lang3.ObjectUtils {
  
  @SuppressWarnings({"rawtypes", "unchecked"})
  public static <T> T mapToObject (BeanUtilsBean bean_utils, Map inmap, T outobj) {
    try {
      bean_utils.populate(outobj, inmap);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new IllegalStateException("Map转对象时出错: " + e.getMessage(), e);
    }
    return outobj;
  }

  @SuppressWarnings({"unchecked"})
  public static <T> Map<String, String> objectToMap(BeanUtilsBean bean_utils, T inobj) {
    if (inobj instanceof Map) return (Map<String, String>)inobj;
    try {
      return bean_utils.describe(inobj);
    } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      throw new IllegalStateException("对象转换Map时出错: " + e.getMessage(), e);
    }
  }

  @SuppressWarnings({"unchecked"})
  public static <T> Map<String, String> objectToMap(BeanUtilsBean bean_utils, T inobj, boolean ignoren_null) {
    if (inobj instanceof Map) return (Map<String, String>)inobj;
    try {
      if (ignoren_null) {
        Map<String, String> origin_map = bean_utils.describe(inobj);
        Map<String, String> return_map = new LinkedHashMap<>();
        for (Map.Entry<String, String> map_entry : origin_map.entrySet()) {
          if (map_entry.getValue() == null) continue;
          return_map.put(map_entry.getKey(), map_entry.getValue());
        }
        return return_map;
      } else {
        return bean_utils.describe(inobj);
      }
    } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      throw new IllegalStateException("对象转换Map时出错: " + e.getMessage(), e);
    }
  }

}
