package com.snz1.jdbc.rest.utils;

import java.io.IOException;
import java.sql.JDBCType;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;

import com.snz1.jdbc.rest.Constants;
import com.snz1.jdbc.rest.data.JdbcQueryRequest;
import com.snz1.jdbc.rest.data.RequestCustomKey;
import com.snz1.jdbc.rest.data.WhereCloumn;

import gateway.api.JsonUtils;

public abstract class RequestUtils {

  // 从请求中获取响应字段
  public static Map<String, JdbcQueryRequest.ResultMeta.ResultColumn> getQueryRequestResultColumns(HttpServletRequest request) {
    Map<String, JdbcQueryRequest.ResultMeta.ResultColumn> columns = new HashMap<>();
    return fetchQueryRequestResultColumns(request, columns);
  }

  // 从请求中提取响应字段
  public static Map<String, JdbcQueryRequest.ResultMeta.ResultColumn> fetchQueryRequestResultColumns(HttpServletRequest request, Map<String, JdbcQueryRequest.ResultMeta.ResultColumn> columns) {
    String[] result_column_names = request.getParameterValues(Constants.RESULT_COLUMNS_ARG);
    if (result_column_names == null) return columns;
    for (String input_column_name : result_column_names) {
      String[] column_split_names = StringUtils.split(input_column_name, ',');
      if (column_split_names == null || column_split_names.length == 0) continue;
      for (String column_name : column_split_names) {
        JdbcQueryRequest.ResultMeta.ResultColumn column = JdbcQueryRequest.ResultMeta.ResultColumn.of(column_name);
        String column_type_arg = String.format("%s.%s.type", Constants.RESULT_COLUMNS_ARG, column_name);
        String column_alias_arg = String.format("%s.%s.alias", Constants.RESULT_COLUMNS_ARG, column_name);
        String type_name = request.getParameter(column_type_arg);
        String alias_name = request.getParameter(column_alias_arg);
        if (StringUtils.isNotBlank(type_name)) {
          JdbcQueryRequest.ResultMeta.ColumnType column_type = JdbcQueryRequest.ResultMeta.ColumnType.valueOf(type_name);
          column.setType(column_type);
        }
        if (StringUtils.isNotBlank(alias_name)) {
          column.setAlias(alias_name);
        }
        columns.put(column_name, column);
      }
    }
    return columns;
  }

  // 从请求中获取排序
  public static List<JdbcQueryRequest.OrderBy> fetchQueryRequestOrderBy(HttpServletRequest request, List<JdbcQueryRequest.OrderBy> order_by) {
    String[] orderby_vals = request.getParameterValues(Constants.ORDERBY_ARG);
    if (orderby_vals == null || orderby_vals.length == 0) {
      return order_by;
    }

    for (String orderby_val : orderby_vals) {
      String order_fields[] = StringUtils.split(orderby_val, ',');
      if (order_fields == null || order_fields.length == 0) continue;
      for (String orderby_field : order_fields) {
        JdbcQueryRequest.OrderBy order_col;
        if (StringUtils.startsWith(orderby_field, "+")) {
          order_col = JdbcQueryRequest.OrderBy.of(orderby_field.substring(1));
          order_col.setSort(JdbcQueryRequest.OrderBy.Sort.asc);
        } else if (StringUtils.startsWith(orderby_field, "-")) {
          order_col = JdbcQueryRequest.OrderBy.of(orderby_field.substring(1));
          order_col.setSort(JdbcQueryRequest.OrderBy.Sort.desc);
        } else {
          order_col = JdbcQueryRequest.OrderBy.of(orderby_field);
        }
        order_by.add(order_col);
      }
    }

    return order_by;
  }

  // 从请求中获取分组
  public static List<JdbcQueryRequest.GroupBy> fetchQueryRequestGroupBy(HttpServletRequest request, List<JdbcQueryRequest.GroupBy> group_by) {
    String[] groupby_vals = request.getParameterValues(Constants.GROUPBY_ARG);
    if (groupby_vals == null || groupby_vals.length == 0) {
      return group_by;
    }

    for (String groupby_val : groupby_vals) {
      String groupby_fields[] = StringUtils.split(groupby_val, ',');
      if (groupby_fields == null || groupby_fields.length == 0) continue;
      for (String groupby_field : groupby_fields) {
        JdbcQueryRequest.GroupBy groupby_col;
        int having_start = groupby_field.indexOf("->>having:", 1);
        if (having_start > 0) {
          groupby_col = JdbcQueryRequest.GroupBy.of(groupby_field.substring(0, having_start));
          String having_val = groupby_field.substring(having_start + 10);
          String parsed_vals[] = StringUtils.split(having_val, ':');
          JDBCType type = null;
          if (parsed_vals == null) {
            throw new IllegalArgumentException(String.format("分组字段%s的条件参数错误：%s", groupby_field, having_val));
          }

          if (parsed_vals.length == 5) {
            type = JDBCType.valueOf(StringUtils.upperCase(parsed_vals[4]));
          } else if (parsed_vals.length != 4) {
            throw new IllegalArgumentException(String.format("分组字段%s的条件参数错误：%s", groupby_field, having_val));
          }

          JdbcQueryRequest.ConditionOperation hop = JdbcQueryRequest.ConditionOperation.valueOf(parsed_vals[2]);
          groupby_col.setHaving(JdbcQueryRequest.Having.of(
            parsed_vals[0], parsed_vals[1], hop, parsed_vals[3]
          ));
          groupby_col.getHaving().setType(type);
        } else {
          groupby_col = JdbcQueryRequest.GroupBy.of(groupby_field);
        }
        group_by.add(groupby_col);
      }
    }

    return group_by;
  }

  // 从请求中获取查询字段描述
  public static JdbcQueryRequest.SelectMeta fetchQueryRequestSelect(HttpServletRequest request, JdbcQueryRequest.SelectMeta select_meta) {
    // 获取是否去除重行
    String distinct_val = request.getParameter(Constants.DISTINCT_ARG);
    if (distinct_val != null) {
      select_meta.setDistinct(
        StringUtils.isBlank(distinct_val) ||
        distinct_val.equals("true")
      );
    }

    // 获取总计
    String count_val = request.getParameter(Constants.COUNT_ARG);
    if (count_val != null) {
      if (StringUtils.isBlank(count_val)) {
        select_meta.setCount("*");
      } else {
        select_meta.setCount(count_val);
      }
    }

    // 获取查询字段
    String[] select_vals = request.getParameterValues(Constants.SELECT_ARG);
    if (select_vals != null && select_vals.length > 0) {
      for (String select_val : select_vals) {
        String select_fields[] = StringUtils.split(select_val, ',');
        if (select_fields == null || select_fields.length == 0) continue;
        for (String select_field : select_fields) {
          JdbcQueryRequest.SelectMeta.SelectColumn select_column;
          int function_start = StringUtils.indexOf(select_field, ':', 1);
          if (function_start > 0) {
            String function_name = select_field.substring(0, function_start);
            select_field = select_field.substring(function_start+1);
            int as_start = StringUtils.indexOf(select_field, "->>", 1);
            String as_name = null;
            if (as_start > 0) {
              as_name = select_field.substring(as_start + 3);
              select_field = select_field.substring(0, as_start);
            }
            select_field = StringUtils.replace(select_field, ":", ",");
            select_column = JdbcQueryRequest.SelectMeta.SelectColumn.of(select_field);
            select_column.setFunction(function_name);
            select_column.setAs(as_name);
          } else {
            select_column = JdbcQueryRequest.SelectMeta.SelectColumn.of(select_field);
          }
          select_meta.getColumns().add(select_column);
        }
      }
    }
    return select_meta;
  }

  // 从请求中获取查询应答描述
  public static JdbcQueryRequest.ResultMeta fetchQueryRequestResultMeta(HttpServletRequest request, JdbcQueryRequest.ResultMeta result_meta) {

    // 获取返回字段定义
    fetchQueryRequestResultColumns(request, result_meta.getColumns());

    // 获取是否返回统计数据
    String result_total_val = request.getParameter(Constants.RESULT_TOTAL_ARG);
    if (result_total_val != null) {
      result_meta.setRow_total(
        StringUtils.isBlank(result_total_val) ||
        result_total_val.equals("true")
      );
    }

    // 获取返回是否包含元信息
    String contain_meta_val = request.getParameter(Constants.RESULT_CONTAIN_META_ARG);
    if (contain_meta_val != null) {
      result_meta.setContain_meta(
        StringUtils.isBlank(contain_meta_val) ||
        contain_meta_val.equals("true")
      );
    }
    
    // 获取返回所有查询字段
    String all_columns_val = request.getParameter(Constants.RESULT_ALL_COLUMNS_ARG);
    if (all_columns_val != null) {
      result_meta.setAll_columns(
        StringUtils.isBlank(all_columns_val) ||
        all_columns_val.equals("true")
      );
    }
    
    // 返回行返回结构
    String row_struct = request.getParameter(Constants.RESULT_ROW_STRUCT_ARG);
    if (StringUtils.isNotBlank(row_struct)) {
      JdbcQueryRequest.ResultMeta.ResultObjectStruct enum_val = JdbcQueryRequest.ResultMeta.ResultObjectStruct.valueOf(row_struct);
      result_meta.setRow_struct(enum_val);
    }

    // 获取开始索引参数
    String offset_val = request.getParameter(Constants.OFFSET_ARG);
    if (StringUtils.isNotBlank(offset_val)) {
      result_meta.setOffset(Long.parseLong(offset_val));
    }

    // 获取返回数量
    String limit_val = request.getParameter(Constants.LIMIT_ARG);
    if (StringUtils.isNotBlank(limit_val)) {
      result_meta.setLimit(Long.parseLong(limit_val));
      if (result_meta.getLimit() > Constants.DEFAULT_MAX_LIMIT) {
        throw new IllegalArgumentException(String.format("返回数量参数limit不能超过%d", Constants.DEFAULT_MAX_LIMIT));
      }
    }

    // 返回单对象
    String signleton_val = request.getParameter(Constants.RESULT_SIGNLETON_ARG);
    if (signleton_val != null) {
      result_meta.setSignleton(
        StringUtils.isBlank(signleton_val) ||
        StringUtils.equals("true", signleton_val)
      );
    }

    return result_meta;
  }

  // 从请求中提取条件描述
  public static List<WhereCloumn> fetchQueryRequestWhereCondition(HttpServletRequest request, List<WhereCloumn> where_condition) {
    Enumeration<String> param_names = request.getParameterNames();
    while(param_names.hasMoreElements()) {
      String param_name = param_names.nextElement();
      if (StringUtils.startsWith(param_name, "_")) continue;
      if (StringUtils.equals(param_name, Constants.OFFSET_ARG)) continue;
      if (StringUtils.equals(param_name, Constants.LIMIT_ARG)) continue;
      String[] param_values = request.getParameterValues(param_name);
      if (param_values == null || param_values.length == 0) continue;
      int type_start = param_name.indexOf('$',1);
      JDBCType column_type = null;
      if (type_start > 0) {
        String type_name = param_name.substring(type_start + 1);
        column_type = JDBCType.valueOf(StringUtils.upperCase(type_name));
        param_name = param_name.substring(0, type_start);
      }
      WhereCloumn where_column = WhereCloumn.of(param_name);
      where_column.setType(column_type);

      for (String param_value : param_values) {
        if (StringUtils.startsWith(param_value, "$")) {
          int operation_end = param_value.indexOf('.', 1);
          if (operation_end < 0) {
            JdbcQueryRequest.ConditionOperation op = JdbcQueryRequest.ConditionOperation.valueOf(param_value);
            if (op.parameter_count() > 0) {
              throw new IllegalArgumentException(String.format("查询条件参数语法不正确: %s=%s", param_name, param_value));
            }
            where_column.addCondition(op, null);
          } else {
            JdbcQueryRequest.ConditionOperation op = JdbcQueryRequest.ConditionOperation.valueOf(param_value.substring(0, operation_end));
            String opval = param_value.substring(operation_end + 1);
            where_column.addCondition(op, opval);
          }
        } else {
          where_column.addCondition(JdbcQueryRequest.ConditionOperation.$eq, param_value);
        }
      }
      where_condition.add(where_column);
    }
    return where_condition;
  }

  // 从请求参数中提取关联查询
  public static List<JdbcQueryRequest.Join> fetchQueryRequestJoin(HttpServletRequest request, List<JdbcQueryRequest.Join> join) {
    String[] join_vals = request.getParameterValues(Constants.JOIN_ARG);
    if (join_vals == null || join_vals.length == 0) {
      return join;
    }

    for (String join_val : join_vals) {
      String join_fields[] = StringUtils.split(join_val, ',');
      if (join_fields == null || join_fields.length == 0) continue;
      for (String join_field : join_fields) {
        String []join_field_data = StringUtils.split(join_field, ':');
        if (join_field_data.length != 3) {
          throw new IllegalArgumentException(String.format("关联参数错误：%s", join_field));
        }
        JdbcQueryRequest.Join join_col = JdbcQueryRequest.Join.of(
          join_field_data[0], join_field_data[1], join_field_data[2]
        );
        join.add(join_col);
      }
    }

    return join;
  }

  // 从请求中获取查询应答描述
  public static JdbcQueryRequest fetchJdbcQueryRequest(HttpServletRequest request, JdbcQueryRequest query) {
    fetchQueryRequestSelect(request, query.getSelect());
    fetchQueryRequestJoin(request, query.getJoin());
    fetchQueryRequestGroupBy(request, query.getGroup_by());
    fetchQueryRequestWhereCondition(request, query.getWhere());
    fetchQueryRequestOrderBy(request, query.getOrder_by());
    fetchQueryRequestResultMeta(request, query.getResult());
    return query;
  }

  // 从请求中获取数据
  public static Object fetchManipulationRequestData(HttpServletRequest request) throws IOException {
    if (request.getParameterNames().hasMoreElements()) {
      Map<String, Object> ret = new LinkedHashMap<String, Object>();
      Enumeration<String> name_enumeration = request.getParameterNames();
      while(name_enumeration.hasMoreElements()) {
        String param_name = name_enumeration.nextElement();
        String param_values[] = request.getParameterValues(param_name);
        if (param_values == null) {
          ret.put(param_name, null);
        } else if (param_values.length == 1) {
          ret.put(param_name, param_values[0]);
        } else {
          ret.put(param_name, Arrays.asList(param_values));
        }
      }
      return ret;
    } else {
      return JsonUtils.fromJson(request.getInputStream(), Object.class);
    }
  }

  // 从请求中获取请求头
  public static RequestCustomKey fetchManipulationRequestCustomKey(HttpServletRequest request, RequestCustomKey custom) {
    // 获取主键定义
    Enumeration<String> primary_keys = request.getHeaders(Constants.HEADER_PRIMARY_KEY_ARG);
    if (primary_keys != null) {
      List<String> custer_primary_list = new LinkedList<>();
      while(primary_keys.hasMoreElements()) {
        String primary_key = primary_keys.nextElement();
        if (StringUtils.isBlank(primary_key)) continue;
        custer_primary_list.add(primary_key);
      }
      if (custer_primary_list.size() > 0) {
        custom.setCustom_key(custer_primary_list.size() == 1 ? custer_primary_list.get(0) : custer_primary_list);
      }
    }

    // 主键分割
    custom.setKey_splitter(request.getHeader(Constants.HEADER_PRIMARY_KEY_ARG));
    if (!custom.hasKey_splitter()) {
      custom.setKey_splitter(Constants.DEFAULT_KEY_SPLITTER);
    }
    return custom;
  }

  // 判断更新是否补丁方式
  public static boolean testRequestUpdateIsPatchMode(HttpServletRequest request) {
    String update_mode = request.getHeader(Constants.HEADER_UPDATE_MODE_ARG);
    return StringUtils.equalsIgnoreCase(update_mode, Constants.UPDATE_PATCH_MODE);
  }

  // 从请求头中提取条件描述
  public static List<WhereCloumn> fetchRequestHeaderWhereCondition(HttpServletRequest request, List<WhereCloumn> where_condition) {
    Enumeration<String> head_names = request.getHeaderNames();
    while(head_names.hasMoreElements()) {
      String param_name = StringUtils.lowerCase(head_names.nextElement());
      if (!StringUtils.startsWith(param_name, Constants.HEADER_WHERE_PREFIX)) continue;
      if (StringUtils.equals(param_name, Constants.HEADER_PRIMARY_KEY_ARG)) continue;
      if (StringUtils.equals(param_name, Constants.HEADER_KEY_SPLITTER_ARG)) continue;
      if (StringUtils.equals(param_name, Constants.HEADER_UPDATE_MODE_ARG)) continue;
      String header_name = param_name.substring(Constants.HEADER_WHERE_PREFIX.length());
      Enumeration<String> header_values = request.getHeaders(param_name);
      param_name = header_name;
      if (header_values == null || !header_values.hasMoreElements()) continue;
      List<String> param_list = new LinkedList<>();
      while(header_values.hasMoreElements()) {
        param_list.add(header_values.nextElement());
      }
      String[] param_values = param_list.toArray(new String[0]);
      if (param_values == null || param_values.length == 0) continue;
      int type_start = param_name.indexOf('$',1);
      JDBCType column_type = null;
      if (type_start > 0) {
        String type_name = param_name.substring(type_start + 1);
        column_type = JDBCType.valueOf(StringUtils.upperCase(type_name));
        param_name = param_name.substring(0, type_start);
      }
      WhereCloumn where_column = WhereCloumn.of(param_name);
      where_column.setType(column_type);

      for (String param_value : param_values) {
        if (StringUtils.startsWith(param_value, "$")) {
          int operation_end = param_value.indexOf('.', 1);
          if (operation_end < 0) {
            JdbcQueryRequest.ConditionOperation op = JdbcQueryRequest.ConditionOperation.valueOf(param_value);
            if (op.parameter_count() > 0) {
              throw new IllegalArgumentException(String.format("查询条件参数语法不正确: %s=%s", param_name, param_value));
            }
            where_column.addCondition(op, null);
          } else {
            JdbcQueryRequest.ConditionOperation op = JdbcQueryRequest.ConditionOperation.valueOf(param_value.substring(0, operation_end));
            String opval = param_value.substring(operation_end + 1);
            where_column.addCondition(op, opval);
          }
        } else {
          where_column.addCondition(JdbcQueryRequest.ConditionOperation.$eq, param_value);
        }
      }
      where_condition.add(where_column);
    }
    return where_condition;
  }

}
