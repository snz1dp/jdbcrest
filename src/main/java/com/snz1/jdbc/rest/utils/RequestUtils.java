package com.snz1.jdbc.rest.utils;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;

import com.snz1.jdbc.rest.Constants;
import com.snz1.jdbc.rest.data.JdbcQueryRequest;

public abstract class RequestUtils {

  public static Map<String, JdbcQueryRequest.ResultMeta.ResultColumn> fetchQueryResultColumns(HttpServletRequest request, Map<String, JdbcQueryRequest.ResultMeta.ResultColumn> columns) {
    String[] result_column_names = request.getParameterValues(Constants.RESULT_COLUMNS_ARG);
    if (result_column_names == null) return columns;
    for (String column_name : result_column_names) {
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
    return columns;
  }

  public static Map<String, JdbcQueryRequest.ResultMeta.ResultColumn> getQueryResultColumns(HttpServletRequest request) {
    Map<String, JdbcQueryRequest.ResultMeta.ResultColumn> columns = new HashMap<>();
    return fetchQueryResultColumns(request, columns);
  }

  public static <T extends JdbcQueryRequest.ResultMeta> T fetchRequestQueryMetaData(HttpServletRequest request, T result_meta) {
    fetchQueryResultColumns(request, result_meta.getColumns());
    String contain_meta_str = request.getParameter(Constants.RESULT_CONTAIN_META_ARG);
    if (contain_meta_str != null) {
      result_meta.setContain_meta(!contain_meta_str.equals("false"));
    }
    String row_struct = request.getParameter(Constants.RESULT_ROW_STRUCT_ARG);
    if (StringUtils.isNotBlank(row_struct)) {
      JdbcQueryRequest.ResultMeta.ResultObjectStruct enum_val = JdbcQueryRequest.ResultMeta.ResultObjectStruct.valueOf(row_struct);
      result_meta.setRow_struct(enum_val);
    }
    String all_columns = request.getParameter(Constants.RESULT_ALL_COLUMNS_ARG);
    if (StringUtils.isNotBlank(all_columns)) {
      result_meta.setAll_columns(Boolean.parseBoolean(all_columns));
    }
    String offset_val = request.getParameter(Constants.OFFSET_ARG);
    if (StringUtils.isNotBlank(offset_val)) {
      result_meta.setOffset(Long.parseLong(offset_val));
    }
    String limit_val = request.getParameter(Constants.LIMIT_ARG);
    if (StringUtils.isNotBlank(limit_val)) {
      result_meta.setLimit(Long.parseLong(limit_val));
      if (result_meta.getLimit() > Constants.DEFAULT_MAX_LIMIT) {
        throw new IllegalArgumentException(String.format("返回数量参数limit不能超过%d", Constants.DEFAULT_MAX_LIMIT));
      }
    }
    return result_meta;
  }

}
