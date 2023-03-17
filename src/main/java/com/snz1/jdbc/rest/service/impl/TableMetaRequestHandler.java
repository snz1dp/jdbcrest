package com.snz1.jdbc.rest.service.impl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.lang3.StringUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.lang.Nullable;

import com.snz1.jdbc.rest.data.JdbcQueryRequest;
import com.snz1.jdbc.rest.data.TableIndexs;
import com.snz1.jdbc.rest.data.TableMeta;
import com.snz1.jdbc.rest.service.AppInfoResolver;
import com.snz1.jdbc.rest.service.SQLDialectProvider;
import com.snz1.jdbc.rest.utils.JdbcUtils;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class TableMetaRequestHandler extends AbstractJdbcQueryRequestHandler<TableMeta> {

  public TableMetaRequestHandler(
    JdbcQueryRequest request,
    SQLDialectProvider sql_dialect_provider,
    AppInfoResolver appInfoResolver
  ) {
    super(request, sql_dialect_provider, null, null, appInfoResolver);
  }

  @Override
  @Nullable
  public TableMeta doInConnection(Connection conn) throws SQLException, DataAccessException {
    JdbcQueryRequest table_query = this.getRequest();
    Object primary_key =  doFetchTablePrimaryKey(conn, table_query.getTable_name());
    TableIndexs table_index = doFetchTableIndexs(conn, table_query.getTable_name());

    String schemas_name = null;
    String table_name = table_query.getTable_name();
    if (this.getSqlDialectProvider().supportSchemas() && StringUtils.contains(table_name, '.')) {
      int first_start = table_name.indexOf(".");
      schemas_name = table_name.substring(0, first_start);
      table_name = table_name.substring(first_start + 1);
    }

    ResultSet rs = conn.getMetaData().getColumns(conn.getCatalog(), schemas_name, table_name, "%");
    try {
      return TableMeta.of(
        rs,
        table_query.getResult(),
        primary_key,
        table_index,
        table_query.getDefinition()
      );
    } finally {
      JdbcUtils.closeResultSet(rs);
    }
  }

}
