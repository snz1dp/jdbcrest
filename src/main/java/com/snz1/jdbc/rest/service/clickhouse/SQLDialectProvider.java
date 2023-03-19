package com.snz1.jdbc.rest.service.clickhouse;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.stereotype.Component;

import com.snz1.jdbc.rest.data.JdbcQueryStatement;
import com.snz1.jdbc.rest.data.ManipulationRequest;
import com.snz1.jdbc.rest.data.JdbcQueryRequest;
import com.snz1.jdbc.rest.service.AbstractSQLDialectProvider;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component("clickhouseSQLDialectProvider")
@ConditionalOnClass(com.clickhouse.jdbc.ClickHouseDriver.class)
public class SQLDialectProvider extends AbstractSQLDialectProvider {

  public static final String NAME = "clickhouse";

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
    if (log.isDebugEnabled()) {
      log.debug("无法动态创建数据库");
    }
    throw new IllegalStateException("无法动态创建数据库");
  }

  @Override
  public PreparedStatement preparePageSelect(Connection conn, JdbcQueryRequest table_query) throws SQLException {
    JdbcQueryStatement base_query = this.createQueryRequestBaseSQL(table_query, false);
    StringBuffer sqlbuf = new StringBuffer();
    long start_time = System.currentTimeMillis();
    try {
      sqlbuf.append(base_query.getSql())
            .append(" LIMIT ")
            .append(table_query.getResult().getOffset())
            .append(",")
            .append(table_query.getResult().getLimit());
      PreparedStatement ps = conn.prepareStatement(sqlbuf.toString());
      if (base_query.hasParameter()) {
        for (int i = 0; i < base_query.getParameters().size(); i++) {
          Object param = base_query.getParameters().get(i);
          ps.setObject(i + 1, param);
        };
      }
      return ps;
    } finally {
      if (log.isDebugEnabled()) {
        log.debug("准备分页查询处理耗时{}毫秒, SQL:\n {}",
          (System.currentTimeMillis() - start_time),
          sqlbuf.toString());
      }
    }
  }

  public PreparedStatement prepareDataInsert(Connection conn, ManipulationRequest insert_request) throws SQLException {
    StringBuffer sqlbuf = new StringBuffer(this.createInsertRequestBaseSQL(insert_request));
    if (insert_request.hasPrimary_key()) {
      // TODO: 实现主键冲突处理
    }
    return conn.prepareStatement(sqlbuf.toString());
  }

}
