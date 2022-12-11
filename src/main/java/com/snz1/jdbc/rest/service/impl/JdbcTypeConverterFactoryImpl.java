package com.snz1.jdbc.rest.service.impl;

import java.lang.reflect.Array;
import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import com.snz1.jdbc.rest.service.JdbcTypeConverter;
import com.snz1.jdbc.rest.service.JdbcTypeConverterFactory;
import com.snz1.utils.JsonUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class JdbcTypeConverterFactoryImpl implements JdbcTypeConverterFactory {

  private List<JdbcTypeConverter> jdbcTypeConverters;

  @EventListener(ContextRefreshedEvent.class)
  public void loadSQLDialectProviders(ContextRefreshedEvent event) {
    final List<JdbcTypeConverter> list = new LinkedList<>();
    event.getApplicationContext().getBeansOfType(JdbcTypeConverter.class).forEach((k, v) -> {
      if (log.isDebugEnabled()) {
        log.debug("已加入{}类型转换器, ID={}", v.getClass().getName(), k);
      }
      list.add(v);
    });
    this.jdbcTypeConverters = new ArrayList<>(list);
  }

  @Override
  public Object convertObject(Object input, JDBCType type) {
    if (input == null) return null;
    for (JdbcTypeConverter converter : this.jdbcTypeConverters) {
      if (!converter.supportType(type)) continue;
      if (log.isDebugEnabled()) {
        log.debug("类型转换: {}=>{}, 输入: {}", type, converter.getClass().getName(), input);
      }
      return converter.convertObject(input);
    }
    if (log.isDebugEnabled()) {
      log.debug("支持{}类型转换: {}", type, input);
    }
    return input;
  }

  @Override
  public Object convertList(List<?> list, JDBCType type) {
    if (list == null) return null;
    List<Object> retlst = new LinkedList<>();
    list.forEach(o -> {
      retlst.add(this.convertObject(o, type));
    });
    return retlst.toArray(new Object[0]);
  }

  @Override
  public Object convertArray(Object input, JDBCType type) {
    if (input == null) return null;
    if (input instanceof String) {
      if (StringUtils.startsWith((String)input, "[")) {
        List<?> list = JsonUtils.fromJson((String)input, List.class);
        return this.convertList(list, type);
      } else {
        List<String> list = Arrays.asList(StringUtils.split((String)input, ','));
        return this.convertList(list, type);
      }
    } else if (input instanceof Set) {
      return this.convertList(new LinkedList<>((Set<?>)input), type);
    } else if (input instanceof List) {
      return this.convertList((List<?>)input, type);
    } else if (input instanceof Array) {
      return this.convertList(Arrays.asList((Array)input), type);
    } else {
      return this.convertList(Arrays.asList(input), type);
    }
  }

}
