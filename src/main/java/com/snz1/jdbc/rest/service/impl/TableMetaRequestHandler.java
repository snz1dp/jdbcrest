package com.snz1.jdbc.rest.service.impl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

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
    Object primary_key =  doFetchTablePrimaryKey(conn, table_query.getCatalog_name(), table_query.getSchema_name(), table_query.getTable_name());
    TableIndexs table_index = doFetchTableIndexs(conn, table_query.getCatalog_name(), table_query.getSchema_name(), table_query.getTable_name());

    ResultSet rs = conn.getMetaData().getColumns(table_query.getCatalog_name(), table_query.getSchema_name(), table_query.getTable_name(), "%");
    try {
      TableMeta meta = TableMeta.of(
        rs,
        table_query.getResult(),
        primary_key,
        table_index,
        table_query.getDefinition()
      );
      if (!this.getSqlDialectProvider().supportSchemas()) {
        meta.setCatalog_name(null);
        meta.setSchema_name(null);
      }
      return meta;
    } finally {
      JdbcUtils.closeResultSet(rs);
    }
  }

}
