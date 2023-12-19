package com.snz1.jdbc.rest.service.impl;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.beanutils.BeanUtilsBean;
import org.springframework.dao.DataAccessException;
import org.springframework.lang.Nullable;

import com.snz1.jdbc.rest.service.AppInfoResolver;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@Deprecated
public class GetPrimaryKeyHandler extends AbstractJdbcQueryRequestHandler<Object> {

  private String table_name;

  private String catalog_name;

  private String schema_name;

  public GetPrimaryKeyHandler(
    String catalog_name, String schema_name, String table_name,
    AppInfoResolver appInfoResolver, BeanUtilsBean bean_utils
  ) {
    super(null, null, null, null, appInfoResolver, bean_utils);
    this.table_name = table_name;
  }

  @Override
  @Nullable
  public Object doInConnection(Connection conn) throws SQLException, DataAccessException {
    return doFetchTablePrimaryKey(conn, catalog_name, schema_name, table_name);
  }

}
