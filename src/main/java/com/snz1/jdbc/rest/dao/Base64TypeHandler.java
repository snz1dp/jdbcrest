package com.snz1.jdbc.rest.dao;

import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.codec.binary.Base64;
import org.apache.ibatis.type.BaseTypeHandler;
import org.apache.ibatis.type.JdbcType;

public class Base64TypeHandler extends BaseTypeHandler<String> {

  @Override
  public void setNonNullParameter(PreparedStatement ps, int i, String parameter, JdbcType jdbcType)
      throws SQLException {
    Connection conn = ps.getConnection();
    byte[] input_bytes = Base64.decodeBase64(parameter);
    if (JdbcType.CLOB.equals(jdbcType)) {
      Clob clob = conn.createClob();
      clob.setString(0, new String(input_bytes));
      ps.setClob(i, clob);
    } else if (JdbcType.BLOB.equals(jdbcType)) {
      Blob blob = conn.createBlob();
      blob.setBytes(0, input_bytes);
      ps.setBlob(i, blob);
    } else if (JdbcType.NCLOB.equals(jdbcType)) {
      NClob nclob = conn.createNClob();
      nclob.setString(0, new String(input_bytes));
      ps.setNClob(i, nclob);
    } else {
      ps.setBytes(i, input_bytes);
    }
  }

  @Override
  public String getNullableResult(ResultSet rs, String columnName) throws SQLException {
    return getBase64String(rs.getBytes(columnName));
  }

  @Override
  public String getNullableResult(ResultSet rs, int columnIndex) throws SQLException {
    return getBase64String(rs.getBytes(columnIndex));
  }

  @Override
  public String getNullableResult(CallableStatement cs, int columnIndex) throws SQLException {
    return getBase64String(cs.getBytes(columnIndex));
  }

  private String getBase64String(byte[] bytes_value) throws SQLException {
    if (bytes_value == null) return null;
    return Base64.encodeBase64String(bytes_value);
  }

}
