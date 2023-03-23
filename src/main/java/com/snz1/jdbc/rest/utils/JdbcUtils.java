package com.snz1.jdbc.rest.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ResultMap;
import org.apache.ibatis.mapping.ResultMapping;
import org.apache.ibatis.mapping.SqlCommandType;
import org.apache.ibatis.scripting.defaults.RawSqlSource;
import org.apache.ibatis.session.Configuration;
import org.postgresql.jdbc.PgArray;

import com.esotericsoftware.yamlbeans.YamlException;
import com.snz1.jdbc.rest.Constants;
import com.snz1.jdbc.rest.data.JdbcMetaData;
import com.snz1.jdbc.rest.data.JdbcProviderMeta;
import com.snz1.jdbc.rest.data.ResultDefinition;
import com.snz1.jdbc.rest.data.SQLClauses;
import com.snz1.jdbc.rest.provider.SQLDialectProvider;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class JdbcUtils extends org.springframework.jdbc.support.JdbcUtils {

  private static Map<String, JdbcProviderMeta> databaseNameMap;

  private static Map<String, SQLDialectProvider> sqlDialectProviderMap;

  static {
    loadDatabaseProviders();
  }

  private static InputStream getDatabase_provider_file() {
    String file_location = "data/database-provider.yaml";
    return Thread.currentThread().getContextClassLoader().getResourceAsStream(file_location);
  }

  private static void loadDatabaseProviders() {
    InputStream provider_file = getDatabase_provider_file();
    Validate.notNull(provider_file, "未设置数据库提供配置文件");
    try {
      sqlDialectProviderMap = Collections.unmodifiableMap(doCreateSQLDialectProviders(
        databaseNameMap = Collections.unmodifiableMap(
          new HashMap<>(doLoadDatabseProviders(provider_file))
        )
      ));
    } catch (Exception e) {
      if (log.isDebugEnabled()) {
        log.debug("加载提供器失败：{}", e.getMessage(), e);
      }
      throw new IllegalStateException("加载提供器失败：" + e.getMessage(), e);
    }
  }

  @SuppressWarnings("unchecked")
  private static Map<String, SQLDialectProvider> doCreateSQLDialectProviders(Map<String, JdbcProviderMeta> meta)
    throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    Map<String, SQLDialectProvider> ret = new HashMap<>(meta.size());
    for (JdbcProviderMeta provider : meta.values()) {
      Class<SQLDialectProvider> provider_clazz = (Class<SQLDialectProvider> )ClassUtils.getClass(provider.getProvider());
      ret.put(provider.getId(), provider_clazz.newInstance());
    }
    return ret;
  }

  private static Map<String, JdbcProviderMeta> doLoadDatabseProviders(InputStream provider_file) throws YamlException, FileNotFoundException {
    InputStream finput = provider_file;
    try {
      JdbcProviderMeta[] dabase_idnames = YamlUtils.fromYaml(finput, JdbcProviderMeta[].class);
      Validate.isTrue(
        dabase_idnames != null && dabase_idnames.length > 0,
        "至少需要一种数据库提供");
      LinkedHashMap<String, JdbcProviderMeta> retmap = new LinkedHashMap<>();
      for (JdbcProviderMeta idname : dabase_idnames) {
        try {
          ClassUtils.getClass(idname.getProvider());
          retmap.put(idname.getName(), idname);
        } catch(ClassNotFoundException e) {
          log.warn("无法找到{}数据库提供类", e);
        }
      }
      return retmap;
    } finally {
      IOUtils.closeQuietly(finput);
    }
  }

  public static JdbcProviderMeta getProviderMeta(String product_name) {
    int start = product_name.indexOf("/");
    if (start > 0) {
      product_name = product_name.substring(0, start);
    }
    return databaseNameMap.get(product_name);
  }

  public static SQLDialectProvider getSQLDialectProvider(String driver_id) {
    return sqlDialectProviderMap.get(driver_id);
  }

  public static JdbcMetaData getJdbcMetaData(Connection conn) throws SQLException {
    JdbcMetaData temp_meta = new JdbcMetaData();
    DatabaseMetaData table_meta =  conn.getMetaData();
    Driver driver = DriverManager.getDriver(conn.getMetaData().getURL());
    JdbcProviderMeta provider = getProviderMeta(table_meta.getDatabaseProductName());
    Validate.notNull(provider, "目前不支持%s", table_meta.getDatabaseProductName());
    temp_meta.setDriver_id(provider.getId());
    temp_meta.setProvider_class(provider.getProvider());
    temp_meta.setProduct_name(table_meta.getDatabaseProductName());
    temp_meta.setProduct_version(table_meta.getDatabaseProductVersion());
    temp_meta.setDriver_name(table_meta.getDriverName());
    temp_meta.setDriver_version(table_meta.getDriverVersion());
    temp_meta.setDriver_class(driver.getClass().getName());
    return temp_meta;
  }

  public static Object getResultSetValue(ResultSet rs, int index) throws SQLException {
    Object ret = org.springframework.jdbc.support.JdbcUtils.getResultSetValue(rs, index);
    if (ret != null) {
      String clazz_name = ret.getClass().getName();
      if (!StringUtils.startsWithAny(clazz_name, "java.")) {
        if (ret instanceof PgArray) {
          ret = ((PgArray)ret).getArray();
        }
      }
    }
    return ret;
  }


	public static List<SQLClauses> loadSQLClausesFromFile(File file, String batch_splitter, Charset defaultCharset) throws IOException {
    List<SQLClauses> sql_batchs = new LinkedList<SQLClauses>();
    List<String> filelines = readURLToLines(file, defaultCharset);
    SQLClauses sql_clauses = new SQLClauses();

    boolean in_annotation = false;
    for (String line : filelines) {
      if (in_annotation) {
        in_annotation = !StringUtils.trim(line).endsWith("*/");
        if (in_annotation) {
          sql_clauses.getNote().append(line);
          sql_clauses.getNote().append("\n");
        } else {
          sql_clauses.getNote().append(StringUtils.stripEnd(line, " ").substring(0, line.length() - 2));
        }
        continue;
      } else if (StringUtils.trim(line).startsWith("/*")) {
        in_annotation = !StringUtils.trim(line).endsWith("*/");
        if (in_annotation) {
          sql_clauses.getNote().append(StringUtils.stripEnd(line, " ").substring(2));
        } else {
          sql_clauses.getNote().append(StringUtils.stripEnd(line, " ").substring(2, StringUtils.stripEnd(line, " ").length() - 2));
        }
        continue;
      } else if (StringUtils.trim(line).startsWith("--")) {
        sql_clauses.getNote().append(StringUtils.stripStart(line, " ").substring(2));
        continue;
      }

      if (line.endsWith(batch_splitter)) {
        sql_clauses.getSql().append(line.substring(0, line.length() - batch_splitter.length()));
        sql_batchs.add(sql_clauses);
        sql_clauses = new SQLClauses();
      } else {
        sql_clauses.getSql().append(line);
        sql_clauses.getSql().append("\n");
      }
    }
    if (StringUtils.isNotBlank(sql_clauses.getSql().toString())) {
      sql_batchs.add(sql_clauses);
    }
    return new ArrayList<SQLClauses>(sql_batchs);
  }

  public static List<String> readURLToLines(File url, Charset defaultCharset) throws IOException {
    InputStream in = new FileInputStream(url);
    try {
      return IOUtils.readLines(in, defaultCharset);
    } finally {
      IOUtils.closeQuietly(in);
    }
  }

  // 分析SQL参数
  public static Map<String, JDBCType> parseSQLParamters(String sql) {
    Map<String, JDBCType> paramters = new LinkedHashMap<>();
    return parseSQLParamters(sql, paramters);
  }

  // 分析SQL参数
  public static Map<String, JDBCType> parseSQLParamters(String sql, Map<String, JDBCType> paramters) {
    int index_start = 1;
    int param_start = sql.indexOf(Constants.SQL_PARAM_PREFIX, index_start);
    while(param_start > 0) {
      int param_end = sql.indexOf(Constants.SQL_PARAM_SUFFIX, param_start);
      if (param_end < 0) break;
      String param_content = sql.substring(param_start + Constants.SQL_PARAM_PREFIX.length(), param_end);
      String param_array[] = StringUtils.split(param_content, ",");
      if (param_array == null || param_array.length == 0 || param_array.length != 2) {
        throw new IllegalArgumentException("参数格式错误");
      }
      JDBCType param_type;
      String type_array[] = StringUtils.split(param_array[1], "=");
      if (type_array == null || type_array.length != 2 || StringUtils.isBlank(param_array[0])) {
        throw new IllegalArgumentException("参数类型定义错误");
      }
      if (!StringUtils.equalsIgnoreCase("jdbcType", StringUtils.trim(type_array[0]))) {
        throw new IllegalArgumentException("参数类型格式错误");
      }
      try {
        param_type = JDBCType.valueOf(type_array[1]);
      } catch(IllegalArgumentException e) {
        throw new IllegalArgumentException(String.format("#{%s}类型错误", param_array[0]), e);
      }

      paramters.put(
        StringUtils.trim(param_array[0]),
        param_type
      );
      index_start = param_end + 1;
      param_start = sql.indexOf(Constants.SQL_PARAM_PREFIX, index_start);
    }

    return paramters;
  }

  public static MappedStatement createMappedStatement(
    String mapped_id,
    Configuration configuration,
    SqlCommandType command_type,
    String sql,
    ResultDefinition result
  ) {
    RawSqlSource sql_source = new RawSqlSource(
      configuration, sql, null
    );

    List<ResultMapping> result_lst = new ArrayList<>();

    if (result != null && result.hasColumn()) {
      for (String column_name : result.getColumns().keySet()) {
        ResultDefinition.ResultColumn result_column = result.getColumns().get(column_name);
        if (Objects.equals(ResultDefinition.ResultType.raw, result_column.getType())) {
          continue;
        }
        ResultMapping.Builder mapping_builder = new ResultMapping.Builder(
          configuration, result_column.getName()
        );
        mapping_builder.column(result_column.getName());
        if (Objects.equals(result_column.getType(), ResultDefinition.ResultType.map)) {
          mapping_builder.javaType(Map.class);
          mapping_builder.typeHandler(new com.snz1.jdbc.rest.dao.MapTypeHandler());
        } else if (Objects.equals(result_column.getType(), ResultDefinition.ResultType.list)) {
          mapping_builder.javaType(List.class);
          mapping_builder.typeHandler(new com.snz1.jdbc.rest.dao.ListTypeHandler());
        } else if (Objects.equals(result_column.getType(), ResultDefinition.ResultType.base64)) {
          mapping_builder.javaType(String.class);
          mapping_builder.typeHandler(new com.snz1.jdbc.rest.dao.Base64TypeHandler());
        }
        result_lst.add(mapping_builder.build());
      }
    }

    ResultMap result_map = new ResultMap.Builder(
      configuration, mapped_id,
      Map.class, result_lst,
      true
    ).build();

    MappedStatement ms = new MappedStatement.Builder(
      configuration, mapped_id, sql_source, command_type
    ).resultMaps(Arrays.asList(result_map)).build();
    return ms;
  }

}
