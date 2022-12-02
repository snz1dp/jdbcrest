package com.snz1.jdbc.rest.service.postgres;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.springframework.stereotype.Component;

import com.snz1.jdbc.rest.data.JdbcQuery;
import com.snz1.jdbc.rest.data.TableQueryRequest;
import com.snz1.jdbc.rest.service.AbstractSQLDialectProvider;

@Component
public class PostgreSQLDialectProvider extends AbstractSQLDialectProvider {

  public static final String NAME = "postgresql";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public PreparedStatement prepareNoRowSelect(Connection conn, TableQueryRequest table_query) throws SQLException {
    JdbcQuery base_query = this.createQueryRequestBaseSQL(table_query);
    StringBuffer sqlbuf = new StringBuffer();
    sqlbuf.append(base_query.getSql()).append(" LIMIT 0");
    PreparedStatement ps = conn.prepareStatement(sqlbuf.toString());
    if (base_query.hasParameter()) {
      for (int i = 0; i < base_query.getParameters().size(); i++) {
        Object param = base_query.getParameters().get(i);
        ps.setObject(i + 1, param);
      };
    }
    return ps;
  }

  @Override
  public PreparedStatement preparePageSelect(Connection conn, TableQueryRequest table_query) throws SQLException {
    JdbcQuery base_query = this.createQueryRequestBaseSQL(table_query);
    StringBuffer sqlbuf = new StringBuffer();
    sqlbuf.append(base_query.getSql())
          .append(" OFFSET ")
          .append(table_query.getResult().getOffset())
          .append(" LIMIT ")
          .append(table_query.getResult().getLimit());
    PreparedStatement ps = conn.prepareStatement(sqlbuf.toString());
    if (base_query.hasParameter()) {
      for (int i = 0; i < base_query.getParameters().size(); i++) {
        Object param = base_query.getParameters().get(i);
        ps.setObject(i + 1, param);
      };
    }
    return ps;
  }

}
