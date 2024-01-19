package com.snz1.jdbc.rest.service.impl;

import java.lang.reflect.Array;
import java.sql.JDBCType;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import javax.annotation.Resource;

import org.apache.commons.lang3.StringUtils;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import com.snz1.jdbc.rest.service.JdbcTypeConverter;
import com.snz1.jdbc.rest.service.JdbcTypeConverterFactory;
import com.snz1.jdbc.rest.service.LoggedUserContext;
import com.snz1.utils.Configurer;
import com.snz1.utils.JsonUtils;
import com.snz1.utils.WebUtils;
import com.snz1.web.security.User;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class JdbcTypeConverterFactoryImpl implements JdbcTypeConverterFactory {

  @Resource
  private LoggedUserContext loggedUserContext;

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

  // 换取内置变量
  private Object fetchBuildInVariable(Object input, JDBCType type) {
    Date curr_time = new Date();
    StringBuffer input_buf = new StringBuffer();
    int var_index = 0;
    int var_start = StringUtils.indexOf((String)input, "${", var_index);
    if (var_start < var_index) return input;
    if (var_start > var_index) {
      input_buf.append(StringUtils.substring((String)input, var_index, var_start));
    }
    int var_end = StringUtils.indexOf((String)input, "}", var_start + 2);
    if (var_end < var_index) return input;
    
    do {
      String var_name = StringUtils.substring((String)input, var_start + 2, var_end);
      var_name = StringUtils.trim(var_name);
      String default_val = null;
      int default_start = StringUtils.indexOf(var_name, ":");
      if (default_start != -1) { // 有缺省值
        default_val = StringUtils.substring(var_name, default_start + 1);
        var_name = StringUtils.substring(var_name, 0, default_start);
      }
      String var_path[] = StringUtils.split(var_name, ".");
      // 假设为原值
      Object var_val = StringUtils.substring((String)input, var_start, var_end + 1);
      if (StringUtils.equals(var_path[0], "system") && var_path.length == 2) {
        String endpath = var_path[1];
        if (StringUtils.equals(endpath, "now")) {
          var_val = new SimpleDateFormat(JsonUtils.JsonDateFormat).format(curr_time);
        } else if (StringUtils.equals(endpath, "timestamp")) {
          var_val = curr_time.getTime();
        }
      } else if (var_path.length == 3 && StringUtils.equals(var_path[0], "app") && StringUtils.equals(var_path[1], "config")) {
        var_val = Configurer.getAppProperty(var_path[2], default_val);
        if (var_val == null) {
          var_val = "";
        }
      } else if (var_path.length == 2 && StringUtils.equals(var_path[0], "client")) {
        String endpath = var_path[1];
        if (StringUtils.equals(endpath, "ip")) {
          var_val = WebUtils.getClientRealIp();
        } else if (StringUtils.equals(endpath, "ua")) {
          var_val = WebUtils.getClientUserAgent();
        }
      } else if (var_path.length == 2 && StringUtils.equals(var_path[0], "user")) {
        String endpath = var_path[1];
        if (StringUtils.equals(endpath, "userid")) {
          if (this.loggedUserContext.isUserLogged()) {
            User userinfo = this.loggedUserContext.getLoggedUser();
            var_val = userinfo.getUserid();
          } else {
            var_val = default_val;
          }
        } else if (StringUtils.equals(endpath, "username")) {
          if (this.loggedUserContext.isUserLogged()) {
            User userinfo = this.loggedUserContext.getLoggedUser();
            var_val = userinfo.getAccount_name();
          } else {
            var_val = default_val;
          }
        } else if (StringUtils.equals(endpath, "nickname")) {
          if (this.loggedUserContext.isUserLogged()) {
            User userinfo = this.loggedUserContext.getLoggedUser();
            var_val = userinfo.getDisplay_name();
          } else {
            var_val = default_val;
          }
        } else if (StringUtils.equals(endpath, "employeeid")) {
          if (this.loggedUserContext.isUserLogged()) {
            User userinfo = this.loggedUserContext.getLoggedUser();
            var_val = userinfo.getEmployeeid();
          } else {
            var_val = default_val;
          }
        } else if (StringUtils.equals(endpath, "idcard")) {
          if (this.loggedUserContext.isUserLogged()) {
            User userinfo = this.loggedUserContext.getLoggedUser();
            var_val = userinfo.getIdcard();
          } else {
            var_val = default_val;
          }
        } else if (StringUtils.equals(endpath, "mobile")) {
          if (this.loggedUserContext.isUserLogged()) {
            User userinfo = this.loggedUserContext.getLoggedUser();
            var_val = userinfo.getRegist_mobile();
          } else {
            var_val = default_val;
          }
        } else if (StringUtils.equals(endpath, "email")) {
          if (this.loggedUserContext.isUserLogged()) {
            User userinfo = this.loggedUserContext.getLoggedUser();
            var_val = userinfo.getRegist_email();
          } else {
            var_val = default_val;
          }
        } else if (StringUtils.equals(endpath, "code")) {
          if (this.loggedUserContext.isUserLogged()) {
            User userinfo = this.loggedUserContext.getLoggedUser();
            var_val = userinfo.getCode();
          } else {
            var_val = default_val;
          }
        }
      }

      input_buf.append(var_val);
      var_index = var_end + 1;
      // 重新开始找
      var_start = StringUtils.indexOf((String)input, "${", var_index);
      if (var_start < var_index) {
        input_buf.append(StringUtils.substring((String)input, var_index));
        break;
      }
      if (var_start > var_index) {
        input_buf.append(StringUtils.substring((String)input, var_index, var_start));
      }
      var_end = StringUtils.indexOf((String)input, "}", var_start + 2);
      if (var_end < var_index) {
        input_buf.append(StringUtils.substring((String)input, var_index));
        break;
      }
    } while(var_end < StringUtils.length((String)input));
    return input_buf.toString();
  }

  @Override
  public Object convertObject(Object input, JDBCType type) {
    if (input == null) return null;

    if (input instanceof String) {
      input = this.fetchBuildInVariable(input, type);
      if (input == null) return null;
    }

    if (type != null) {
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
    }
    if (input instanceof Enum) {
      return input.toString();
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
  @SuppressWarnings("unchecked")
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
    } else if (input instanceof Iterable) {
      List<Object> input_list = new LinkedList<>();
      ((Iterable<Object>)input).forEach(item -> {
        input_list.add(item);
      });
      return this.convertList(input_list, type);
    } else if (input.getClass().isArray()) {
      List<Object> input_list = new LinkedList<>();
      for (int i = 0; i < Array.getLength(input); i++) {
        input_list.add(Array.get(input, i));
      }
      return this.convertList(input_list, type);
    } else {
      return this.convertList(Arrays.asList(input), type);
    }
  }

}
