package com.snz1.jdbc.rest.service.postgres;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.springframework.stereotype.Component;

import com.snz1.jdbc.rest.data.JdbcQuery;
import com.snz1.jdbc.rest.data.JdbcInsertRequest;
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
    JdbcQuery base_query = this.createQueryRequestBaseSQL(table_query, false);
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
    JdbcQuery base_query = this.createQueryRequestBaseSQL(table_query, false);
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

  public PreparedStatement prepareDataInsert(Connection conn, JdbcInsertRequest insert_request) throws SQLException {
    StringBuffer sqlbuf = new StringBuffer(this.createInsertRequestBaseSQL(insert_request));
    if (insert_request.hasPrimary_key()) { // 添加冲突处理
      sqlbuf.append(" on conflict(");
      if (insert_request.testComposite_key()) {
        boolean append = false;
        List<?> keydata = (List<?>)insert_request.getPrimary_key();
        for (int i = 0; i < keydata.size(); i++) {
          if (append) {
            sqlbuf.append(", ");
          } else {
            append = true;
          }
          sqlbuf.append(keydata.get(i));
        }
      } else {
        sqlbuf.append(insert_request.getPrimary_key());
      }
      sqlbuf.append(") do nothing");
    }
    return conn.prepareStatement(sqlbuf.toString());
  }

}
