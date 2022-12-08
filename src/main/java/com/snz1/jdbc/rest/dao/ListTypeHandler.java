package com.snz1.jdbc.rest.dao;

import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.type.BaseTypeHandler;
import org.apache.ibatis.type.JdbcType;

import com.snz1.utils.JsonUtils;

public class ListTypeHandler extends BaseTypeHandler<List<String>> {

  @Override
  public void setNonNullParameter(PreparedStatement ps, int i, List<String> parameter, JdbcType jdbcType)
      throws SQLException {
    if (JdbcType.ARRAY.equals(jdbcType)) {
      Connection conn = ps.getConnection();
      String[] string_array = parameter.toArray(new String[0]);
      Array array_value = conn.createArrayOf("varchar", string_array);
      ps.setArray(i, array_value);
    } else {
      ps.setString(i, JsonUtils.toJson(parameter));
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<String> getNullableResult(ResultSet rs, String columnName) throws SQLException {
    JdbcType jdbcType = JdbcType.ARRAY;
    for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
      if (StringUtils.equalsIgnoreCase(columnName, rs.getMetaData().getColumnName(i))) {
        jdbcType = JdbcType.forCode(rs.getMetaData().getColumnType(i));
        break;
      }
    }
    if (JdbcType.ARRAY.equals(jdbcType)) {
      return getStringList(rs.getArray(columnName));
    } else {
      String result_value = rs.getString(columnName);
      if (result_value == null) return null;
      return JsonUtils.fromJson(result_value, List.class);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<String> getNullableResult(ResultSet rs, int columnIndex) throws SQLException {
    JdbcType jdbcType = JdbcType.forCode(rs.getMetaData().getColumnType(columnIndex));
    if (JdbcType.ARRAY.equals(jdbcType)) {
      return getStringList(rs.getArray(columnIndex));
    } else {
      String result_value = rs.getString(columnIndex);
      if (result_value == null) return null;
      return JsonUtils.fromJson(result_value, List.class);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<String> getNullableResult(CallableStatement cs, int columnIndex) throws SQLException {
    JdbcType jdbcType = JdbcType.forCode(cs.getMetaData().getColumnType(columnIndex));
    if (JdbcType.ARRAY.equals(jdbcType)) {
      return getStringList(cs.getArray(columnIndex));
    } else {
      String result_value = cs.getString(columnIndex);
      if (result_value == null) return null;
      return JsonUtils.fromJson(result_value, List.class);
    }
  }

  private List<String> getStringList(Array array_value) throws SQLException {
    if (array_value == null) return null;
    List<String> ret = new LinkedList<String>();
    for (Object obj : (Object[])array_value.getArray()) {
      ret.add((String)obj);
    }
    return ret;
  }

}
