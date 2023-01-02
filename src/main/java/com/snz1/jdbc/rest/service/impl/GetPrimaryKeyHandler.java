package com.snz1.jdbc.rest.service.impl;

import java.sql.Connection;
import java.sql.SQLException;

import org.springframework.dao.DataAccessException;
import org.springframework.lang.Nullable;

import com.snz1.jdbc.rest.service.AppInfoResolver;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class GetPrimaryKeyHandler extends AbstractJdbcQueryRequestHandler<Object> {

  private String table_name;

  public GetPrimaryKeyHandler(String table_name, AppInfoResolver appInfoResolver) {
    super(null, null, null, null, appInfoResolver);
    this.table_name = table_name;
  }

  @Override
  @Nullable
  public Object doInConnection(Connection conn) throws SQLException, DataAccessException {
    return doFetchTablePrimaryKey(conn, table_name);
  }

}
