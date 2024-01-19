package com.snz1.jdbc.rest.service.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.beanutils.BeanUtilsBean;
import org.springframework.dao.DataAccessException;
import org.springframework.lang.Nullable;

import com.snz1.jdbc.rest.data.JdbcQueryRequest;
import com.snz1.jdbc.rest.data.JdbcQueryResponse;
import com.snz1.jdbc.rest.data.JdbcQueryStatement;
import com.snz1.jdbc.rest.data.TableIndexs;
import com.snz1.jdbc.rest.provider.SQLDialectProvider;
import com.snz1.jdbc.rest.service.AppInfoResolver;
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
    AppInfoResolver appInfoResolver,
    BeanUtilsBean bean_utils
  ) {
    super(request, sql_dialect_provider, null, null, appInfoResolver, bean_utils);
  }

  @Override
  @Nullable
  public JdbcQueryResponse<List<Object>> doInConnection(Connection conn) throws SQLException, DataAccessException {
    JdbcQueryRequest table_query = this.getRequest();
    Object primary_key = null;
    TableIndexs table_index = null;
    if (table_query.hasTable_meta()) {
      primary_key = table_query.getTable_meta().getPrimary_key();
      table_index = new TableIndexs(table_query.getTable_meta().getUnique_indexs(), table_query.getTable_meta().getNormal_indexs());
    } else {
      primary_key = doFetchTablePrimaryKey(conn, table_query.getCatalog_name(), table_query.getSchema_name(), table_query.getTable_name());
      table_index = doFetchTableIndexs(conn, table_query.getCatalog_name(), table_query.getSchema_name(), table_query.getTable_name());
    }

    // 如果有默认配置字段则设置到请求对象中
    if (table_query.hasDefinition() && table_query.getDefinition().hasDefault_columns()) {
      table_query.getDefinition().getDefault_columns().forEach((k, v) -> {
        if (table_query.getResult().getColumns().containsKey(k)) return;
        table_query.getResult().getColumns().put(k, v.clone());
      });
    }

    SQLDialectProvider sql_dialect_provider = this.getSqlDialectProvider();
    JdbcQueryStatement sts = sql_dialect_provider.preparePageSelect(table_query);
    PreparedStatement ps = null;
    try {
      ps = conn.prepareStatement(sts.getSql());
      int i = 1;
      for (Object param : sts.getParameters()) {
        ps.setObject(i, param);
        i = i + 1;
      }
      ResultSet rs = null;
      try {
        rs = ps.executeQuery();
        return doFetchResultSet(
          rs, table_query.getResult(),
          primary_key, table_index,
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

