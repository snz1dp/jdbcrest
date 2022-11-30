package com.snz1.jdbc.rest.service.postgres;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.ibatis.jdbc.SQL;
import org.springframework.stereotype.Component;

import com.snz1.jdbc.rest.data.TableQueryRequest;
import com.snz1.jdbc.rest.service.SQLDialectProvider;

@Component
public class PostgreSQLDialectProvider implements SQLDialectProvider {

  public static final String NAME = "postgresql";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public PreparedStatement prepareZeroSelect(Connection conn, TableQueryRequest table_query) throws SQLException {
    SQL sql = new SQL();
    sql.FROM(table_query.getTable_name());
    if (table_query.getResult_meta().isAll_columns()) {
      sql.SELECT("*");
    } else {
      table_query.getResult_meta().getColumns().forEach((k, v) -> {
        sql.SELECT(k);
      });
    }
    StringBuffer sqlbuf = new StringBuffer();
    sqlbuf.append(sql.toString()).append(" LIMIT 0");
    return conn.prepareStatement(sqlbuf.toString());
  }

  @Override
  public PreparedStatement prepareTableSelect(Connection conn, TableQueryRequest table_query) throws SQLException {
    SQL sql = new SQL();
    sql.FROM(table_query.getTable_name());
    if (table_query.getResult_meta().isAll_columns() || table_query.getResult_meta().getColumns().size() == 0) {
      sql.SELECT("*");
    } else {
      table_query.getResult_meta().getColumns().forEach((k, v) -> {
        sql.SELECT(k);
      });
    }
    StringBuffer sqlbuf = new StringBuffer();
    sqlbuf.append(sql.toString())
          .append(" OFFSET ")
          .append(table_query.getResult_meta().getOffset())
          .append(" LIMIT ")
          .append(table_query.getResult_meta().getLimit());
    return conn.prepareStatement(sqlbuf.toString());
  }

}
