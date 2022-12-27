package com.snz1.jdbc.rest.service.impl;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.springframework.dao.DataAccessException;
import org.springframework.lang.Nullable;

import com.snz1.jdbc.rest.data.JdbcQueryResponse;
import com.snz1.jdbc.rest.utils.JdbcUtils;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class GetCatalogsHandler extends AbstractJdbcQueryRequestHandler<JdbcQueryResponse<List<Object>>> {

  public GetCatalogsHandler() {
    super(null, null, null, null);
  }

  @Override
  @Nullable
  public JdbcQueryResponse<List<Object>> doInConnection(Connection conn) throws SQLException, DataAccessException {
    DatabaseMetaData table_meta =  conn.getMetaData();
    ResultSet rs = table_meta.getCatalogs();
    try {
      return doFetchResultSet(rs, null, null, null, null);
    } finally {
      JdbcUtils.closeResultSet(rs);
    }
  }

}
