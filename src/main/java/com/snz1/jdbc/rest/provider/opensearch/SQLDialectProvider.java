package com.snz1.jdbc.rest.provider.opensearch;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

import com.snz1.jdbc.rest.data.JdbcQueryStatement;
import com.snz1.jdbc.rest.data.ManipulationRequest;
import com.snz1.jdbc.rest.provider.AbstractSQLDialectProvider;
import com.snz1.jdbc.rest.data.JdbcQueryRequest;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SQLDialectProvider extends AbstractSQLDialectProvider {

  static {
    try {
      addObjectTypeConverters();
    } catch(Throwable e) {
      log.warn("加载opensearch类型转换出错: {}", e.getMessage());
    }
  }

  @SuppressWarnings({"unchecked", "rawTypes"})
  protected static void addObjectTypeConverters() throws IllegalArgumentException, IllegalAccessException, ClassNotFoundException {
    Class<?> type_converters_clazz = ClassUtils.getClass("org.opensearch.jdbc.types.TypeConverters");
    if (type_converters_clazz == null) return;
    Field map_field = FieldUtils.getDeclaredField(type_converters_clazz, "tcMap", true);
    if (map_field == null || map_field.get(null) == null) return;
    Map<JDBCType, Object> map_value = (Map<JDBCType, Object>)map_field.get(null);
    map_value.put(JDBCType.STRUCT, new ObjectTypeConverter());
  }

  @Override
  public boolean checkTableExisted() {
    return false;
  }

  @Override
  public boolean supportSchemas() {
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
  public PreparedStatement preparePageSelect(Connection conn, JdbcQueryRequest table_query) throws SQLException {
    JdbcQueryStatement base_query = this.createQueryRequestBaseSQL(table_query, false);
    StringBuffer sqlbuf = new StringBuffer();
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
  }

  public PreparedStatement prepareDataInsert(Connection conn, ManipulationRequest insert_request) throws SQLException {
    StringBuffer sqlbuf = new StringBuffer(this.createInsertRequestBaseSQL(insert_request));
    if (insert_request.hasPrimary_key()) {
      // TODO: 添加冲突处理
    }
    return conn.prepareStatement(sqlbuf.toString());
  }

}
