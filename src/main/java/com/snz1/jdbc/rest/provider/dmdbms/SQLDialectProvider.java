package com.snz1.jdbc.rest.provider.dmdbms;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
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
      StringBuffer ignore_sql = new StringBuffer(" /*+ IGNORE_ROW_ON_DUPKEY_INDEX(");
      ignore_sql.append(insert_request.getFullTableName()).append("(");
      if (insert_request.testComposite_key()) {
        boolean append = false;
        for (Object val : (List<?>)insert_request.getPrimary_key()) {
          if (append) {
            ignore_sql.append(",");
          } else {
            append = true;
          }
          ignore_sql.append("\"").append(val).append("\"");
        }
        ignore_sql.append(insert_request.getPrimary_key());
      } else {
        ignore_sql.append("\"").append(insert_request.getPrimary_key()).append("\"");
      }
      ignore_sql.append(")) */");
      sqlbuf.insert(6, ignore_sql.toString()); // 在insert后插入ignore
    }
    return conn.prepareStatement(sqlbuf.toString());
  }

}
