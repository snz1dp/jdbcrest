package com.snz1.jdbc.rest.dao;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.type.BaseTypeHandler;
import org.apache.ibatis.type.JdbcType;

import com.snz1.utils.JsonUtils;

public class MapTypeHandler extends BaseTypeHandler<Map<String, Object>> {

  @Override
  public void setNonNullParameter(PreparedStatement ps, int i, Map<String, Object> parameter, JdbcType jdbcType)
      throws SQLException {
    ps.setString(i, JsonUtils.toJson(parameter));
  }

  @Override
  public Map<String, Object> getNullableResult(ResultSet rs, String columnName) throws SQLException {
    return toMap(rs.getString(columnName));
  }

  @Override
  public Map<String, Object> getNullableResult(ResultSet rs, int columnIndex) throws SQLException {
    return toMap(rs.getString(columnIndex));
  }

  @Override
  public Map<String, Object> getNullableResult(CallableStatement cs, int columnIndex) throws SQLException {
    return toMap(cs.getString(columnIndex));
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> toMap(String val) throws SQLException {
    if (StringUtils.isBlank(val)) return Collections.EMPTY_MAP;
    return JsonUtils.fromJson(val, Map.class);
  }

}
