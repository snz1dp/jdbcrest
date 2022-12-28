package com.snz1.jdbc.rest.service.impl;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
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
public class TestTableExistedHandler extends AbstractJdbcQueryRequestHandler<Boolean> {

  private String table_name;
  private String types[];

  public TestTableExistedHandler(String table_name, String ...types) {
    super(null, null, null, null);
    this.table_name = table_name;
    this.types = types;
  }

  @Override
  @Nullable
  public Boolean doInConnection(Connection conn) throws SQLException, DataAccessException {
    String schemas_name = null;
    String table_name = this.table_name;
    if (StringUtils.contains(table_name, ".")) {
      String string_array[] = StringUtils.split(table_name, ".");
      schemas_name = string_array[0];
      table_name = string_array[1];
    }
    DatabaseMetaData table_meta = conn.getMetaData();
    ResultSet rs = table_meta.getTables(null, schemas_name, table_name, types != null && types.length > 0 ? types : null);
    try {
      JdbcQueryResponse<List<Object>> table_ret = doFetchResultSet(rs, null, null, null, null);
      return table_ret != null && table_ret.data.size() > 0;
    } finally {
      JdbcUtils.closeResultSet(rs);
    }
  }

}
