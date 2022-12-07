package com.snz1.jdbc.rest.dao;

import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import org.apache.ibatis.type.BaseTypeHandler;
import org.apache.ibatis.type.JdbcType;

public class ListTypeHandler extends BaseTypeHandler<List<String>> {

  @Override
  public void setNonNullParameter(PreparedStatement ps, int i, List<String> parameter, JdbcType jdbcType)
      throws SQLException {
    Connection conn = ps.getConnection();
    String[] string_array = parameter.toArray(new String[0]);
    Array array_value = conn.createArrayOf("varchar", string_array);
    ps.setArray(i, array_value);
  }

  @Override
  public List<String> getNullableResult(ResultSet rs, String columnName) throws SQLException {
    return getStringList(rs.getArray(columnName));
  }

  @Override
  public List<String> getNullableResult(ResultSet rs, int columnIndex) throws SQLException {
    return getStringList(rs.getArray(columnIndex));
  }

  @Override
  public List<String> getNullableResult(CallableStatement cs, int columnIndex) throws SQLException {
    return getStringList(cs.getArray(columnIndex));
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
