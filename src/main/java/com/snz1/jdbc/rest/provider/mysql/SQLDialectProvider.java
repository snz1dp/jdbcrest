package com.snz1.jdbc.rest.provider.mysql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

import com.snz1.jdbc.rest.data.JdbcQueryStatement;
import com.snz1.jdbc.rest.data.ManipulationRequest;
import com.snz1.jdbc.rest.provider.AbstractSQLDialectProvider;
import com.snz1.jdbc.rest.data.JdbcQueryRequest;
import com.snz1.jdbc.rest.utils.JdbcUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SQLDialectProvider extends AbstractSQLDialectProvider {

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
      result = stmt.executeQuery(String.format("SHOW DATABASE LIKE '%s';", databaseName));
      if (!result.next()) {
        String createSQL = String.format("CREATE DATABASE IF NOT EXISTS %s;", databaseName);
        if (log.isDebugEnabled()) {} {
          log.debug("执行SQL语句: " + createSQL);
        }
        stmt.execute(createSQL);
      }
    } catch(SQLException e) {
      throw new IllegalStateException("创建数据库失败: " + e.getMessage(), e);
    } finally {
      JdbcUtils.closeResultSet(result);
      JdbcUtils.closeStatement(stmt);
    }
  }

  @Override
  public JdbcQueryStatement preparePageSelect(JdbcQueryRequest table_query) {
    JdbcQueryStatement base_query = this.createQueryRequestBaseSQL(table_query, false);
    StringBuffer sqlbuf = new StringBuffer();
    sqlbuf.append(base_query.getSql())
          .append(" OFFSET ")
          .append(table_query.getResult().getOffset())
          .append(" LIMIT ")
          .append(table_query.getResult().getLimit());
    base_query.setSql(sqlbuf.toString());
    return base_query;
  }

  public JdbcQueryStatement prepareDataInsert(ManipulationRequest insert_request) {
    JdbcQueryStatement base_query = super.prepareDataInsert(insert_request);
    if (insert_request.hasPrimary_key()) {
      StringBuffer sqlbuf = new StringBuffer(base_query.getSql());
      // TODO: 添加冲突处理，未测试
      sqlbuf.insert(6, " ignore"); // 在insert后插入ignore
      base_query.setSql(sqlbuf.toString());
    }
    return base_query;
  }

}
