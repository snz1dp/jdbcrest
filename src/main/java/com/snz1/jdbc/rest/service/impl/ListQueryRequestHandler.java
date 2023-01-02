package com.snz1.jdbc.rest.service.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.springframework.dao.DataAccessException;
import org.springframework.lang.Nullable;

import com.snz1.jdbc.rest.data.JdbcQueryRequest;
import com.snz1.jdbc.rest.data.JdbcQueryResponse;
import com.snz1.jdbc.rest.data.TableIndex;
import com.snz1.jdbc.rest.service.AppInfoResolver;
import com.snz1.jdbc.rest.service.SQLDialectProvider;
import com.snz1.jdbc.rest.utils.JdbcUtils;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class ListQueryRequestHandler extends AbstractJdbcQueryRequestHandler<JdbcQueryResponse<List<Object>>> {

  public ListQueryRequestHandler(
    JdbcQueryRequest request,
    SQLDialectProvider sql_dialect_provider,
    AppInfoResolver appInfoResolver
  ) {
    super(request, sql_dialect_provider, null, null, appInfoResolver);
  }

  @Override
  @Nullable
  public JdbcQueryResponse<List<Object>> doInConnection(Connection conn) throws SQLException, DataAccessException {
    JdbcQueryRequest table_query = this.getRequest();
    Object primary_key = null;
    List<TableIndex> unique_index = null;
    if (table_query.hasTable_meta()) {
      primary_key = table_query.getTable_meta().getPrimary_key();
      unique_index = table_query.getTable_meta().getUnique_indexs();
    } else {
      primary_key = doFetchTablePrimaryKey(conn, table_query.getTable_name());
      unique_index = doFetchTableUniqueIndex(conn, table_query.getTable_name());
    }
    SQLDialectProvider sql_dialect_provider = this.getSqlDialectProvider();
    PreparedStatement ps = sql_dialect_provider.preparePageSelect(conn, table_query);
    try {
      ResultSet rs = null;
      try {
        rs = ps.executeQuery();
        return doFetchResultSet(
          rs, table_query.getResult(),
          primary_key, unique_index,
          table_query.getDefinition()
        );
      } finally {
        if (rs != null) {
          JdbcUtils.closeResultSet(rs);
        }
      }
    } finally {
      JdbcUtils.closeStatement(ps);
    }
  }

}

