package com.snz1.jdbc.rest.service.impl;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.springframework.dao.DataAccessException;
import org.springframework.lang.Nullable;

import com.snz1.jdbc.rest.data.JdbcQueryResponse;
import com.snz1.jdbc.rest.data.Page;
import com.snz1.jdbc.rest.data.ResultDefinition;
import com.snz1.jdbc.rest.service.AppInfoResolver;
import com.snz1.jdbc.rest.utils.JdbcUtils;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class GetTablesHandler extends AbstractJdbcQueryRequestHandler<JdbcQueryResponse<Page<Object>>> {

  private String catalog;

  private ResultDefinition return_meta;

  private String schema_pattern;

  private String table_name_pattern;

  private String types[];

  private Long offset;

  private Long limit;

  public GetTablesHandler(
    AppInfoResolver appInfoResolver,
    ResultDefinition return_meta,
    String catalog, String schema_pattern,
    String table_name_pattern,
    Long offset, Long limit, String...types
  ) {
    super(null, null, null, null, appInfoResolver);
    this.catalog = catalog;
    this.return_meta = return_meta;
    this.catalog = catalog;
    this.schema_pattern = schema_pattern;
    this.table_name_pattern = table_name_pattern;
    this.types = types;
    this.offset = offset;
    this.limit = limit;
  }

  @Override
  @Nullable
  public JdbcQueryResponse<Page<Object>> doInConnection(Connection conn) throws SQLException, DataAccessException {
    DatabaseMetaData table_meta = conn.getMetaData();
    ResultSet rs = table_meta.getTables(catalog, schema_pattern, table_name_pattern, types != null && types.length > 0 ? types : null);
    try {
      JdbcQueryResponse<Page<Object>> resp = new JdbcQueryResponse<>();
      Page<Object> page = new Page<Object>();
      JdbcQueryResponse<List<Object>> jt = doFetchResultSet(rs, return_meta, null, null, null, offset, limit);
      page.setData(jt.getData());
      page.setOffset(offset);
      resp.setData(page);
      return resp;
    } finally {
      JdbcUtils.closeResultSet(rs);
    }
  }

}
