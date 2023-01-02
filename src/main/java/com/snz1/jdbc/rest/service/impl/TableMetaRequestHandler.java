package com.snz1.jdbc.rest.service.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.springframework.dao.DataAccessException;
import org.springframework.lang.Nullable;

import com.snz1.jdbc.rest.data.JdbcQueryRequest;
import com.snz1.jdbc.rest.data.TableIndex;
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
    SQLDialectProvider sql_dialect_provider = this.getSqlDialectProvider();
    Object primary_key =  doFetchTablePrimaryKey(conn, table_query.getTable_name());
    List<TableIndex> unique_index = doFetchTableUniqueIndex(conn, table_query.getTable_name());
    PreparedStatement ps = sql_dialect_provider.prepareNoRowSelect(conn, table_query);
    try {
      ResultSet rs = ps.executeQuery();
      try {
        rs = ps.getResultSet();
        return TableMeta.of(
          rs.getMetaData(),
          table_query.getResult(),
          primary_key,
          unique_index,
          table_query.getDefinition()
        );
      } finally {
        JdbcUtils.closeResultSet(rs);
      }
    } finally {
      JdbcUtils.closeStatement(ps);
    }
  }

}
