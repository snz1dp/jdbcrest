package com.snz1.jdbc.rest.service.postgresql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.stereotype.Component;

import com.snz1.jdbc.rest.data.JdbcQueryStatement;
import com.snz1.jdbc.rest.data.ManipulationRequest;
import com.snz1.jdbc.rest.data.JdbcQueryRequest;
import com.snz1.jdbc.rest.service.AbstractSQLDialectProvider;
import com.snz1.jdbc.rest.utils.JdbcUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component("postgresqlSQLDialectProvider")
public class SQLDialectProvider extends AbstractSQLDialectProvider {

  public static final String NAME = "postgresql";

  @Override
  public String getId() {
    return NAME;
  }

  public static void setupDatabaseEnvironment(ConfigurableEnvironment environment) {
    Map<String, Object> database_properties = new HashMap<>();
    database_properties.put("DB_VALIDATION_QUERY", "select 1");
		environment.getPropertySources().addLast(new MapPropertySource("jdbcrest", database_properties));
  }

  public static void createDatabaseIfNotExisted(
    Connection conn, String databaseName, String databaseUsername, String databasePassword
  ) {
    ResultSet result = null;
    Statement stmt = null;
    try {
      stmt = conn.createStatement();
      result = stmt.executeQuery(String.format("SELECT u.datname FROM pg_catalog.pg_database u where u.datname='%s';", databaseName));
      if (!result.next()) {
        String createSQL = String.format("CREATE DATABASE %s WITH OWNER %s;", databaseName, databaseUsername);
        if (log.isDebugEnabled()) {} {
          log.debug("执行SQL语句: " + createSQL);
        }
        stmt.execute(createSQL);
      }
    } catch(SQLException e) {
      if (!StringUtils.contains(e.getMessage(), "already exists")) {
        throw new IllegalStateException("创建数据库失败: " + e.getMessage(), e);
      }
    } finally {
      JdbcUtils.closeResultSet(result);
      JdbcUtils.closeStatement(stmt);
    }
  }

  @Override
  public PreparedStatement preparePageSelect(Connection conn, JdbcQueryRequest table_query) throws SQLException {
    JdbcQueryStatement base_query = this.createQueryRequestBaseSQL(table_query, false);
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

  public PreparedStatement prepareDataInsert(Connection conn, ManipulationRequest insert_request) throws SQLException {
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
