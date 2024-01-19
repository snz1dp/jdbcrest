package com.snz1.jdbc.rest.provider.trino;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

import com.snz1.jdbc.rest.data.JdbcQueryStatement;
import com.snz1.jdbc.rest.data.ManipulationRequest;
import com.snz1.jdbc.rest.provider.AbstractSQLDialectProvider;
import com.snz1.jdbc.rest.data.JdbcQueryRequest;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SQLDialectProvider extends AbstractSQLDialectProvider {

  @Override
  public boolean supportCountAnyColumns() {
    return false;
  }

  public static void setupDatabaseEnvironment(ConfigurableEnvironment environment) {
    Map<String, Object> database_properties = new HashMap<>();
    database_properties.put("DB_VALIDATION_QUERY", "select 1");
		environment.getPropertySources().addLast(new MapPropertySource("jdbcrest", database_properties));
  }

  public static void createDatabaseIfNotExisted(
    Connection conn, String databaseName, String databaseUsername, String databasePassword
  ) {
    if (log.isDebugEnabled()) {
      log.debug("无法动态创建数据库");
    }
    throw new IllegalStateException("无法动态创建数据库");
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
    if (insert_request.hasPrimary_key()) { // 添加冲突处理
      StringBuffer sqlbuf = new StringBuffer(base_query.getSql());
      // TODO: 冲突处理未验证
      StringBuffer ignore_sql = new StringBuffer(" OVERWRITE");
      sqlbuf.delete(7, 10).insert(6, ignore_sql.toString());
      base_query.setSql(sqlbuf.toString());
    }
    return base_query;
  }

}
