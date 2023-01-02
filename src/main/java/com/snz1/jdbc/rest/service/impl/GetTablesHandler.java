package com.snz1.jdbc.rest.service.impl;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.springframework.dao.DataAccessException;
import org.springframework.lang.Nullable;

import com.snz1.jdbc.rest.data.JdbcQueryResponse;
import com.snz1.jdbc.rest.data.ResultDefinition;
import com.snz1.jdbc.rest.service.AppInfoResolver;
import com.snz1.jdbc.rest.utils.JdbcUtils;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class GetTablesHandler extends AbstractJdbcQueryRequestHandler<JdbcQueryResponse<List<Object>>> {

  private String catalog;

  private ResultDefinition return_meta;

  private String schema_pattern;

  private String table_name_pattern;

  private String types[];

  public GetTablesHandler(
    AppInfoResolver appInfoResolver,
    ResultDefinition return_meta,
    String catalog, String schema_pattern,
    String table_name_pattern, String...types
  ) {
    super(null, null, null, null, appInfoResolver);
    this.catalog = catalog;
    this.return_meta = return_meta;
    this.catalog = catalog;
    this.schema_pattern = schema_pattern;
    this.table_name_pattern = table_name_pattern;
    this.types = types;
  }

  @Override
  @Nullable
  public JdbcQueryResponse<List<Object>> doInConnection(Connection conn) throws SQLException, DataAccessException {
    DatabaseMetaData table_meta = conn.getMetaData();
    ResultSet rs = table_meta.getTables(catalog, schema_pattern, table_name_pattern, types != null && types.length > 0 ? types : null);
    try {
      return doFetchResultSet(rs, return_meta, null, null, null);
    } finally {
      JdbcUtils.closeResultSet(rs);
    }
  }

}
