package com.snz1.jdbc.rest.service.impl;

import java.io.InputStream;
import java.sql.JDBCType;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.ibatis.mapping.SqlCommandType;
import org.apache.ibatis.session.SqlSessionFactory;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.stereotype.Service;

import com.snz1.jdbc.rest.Constants;
import com.snz1.jdbc.rest.RunConfig;
import com.snz1.jdbc.rest.data.ResultDefinition;
import com.snz1.jdbc.rest.data.SQLClauses;
import com.snz1.jdbc.rest.data.SQLServiceDefinition;
import com.snz1.jdbc.rest.service.SQLServiceRegistry;
import com.snz1.jdbc.rest.utils.JdbcUtils;
import com.snz1.jdbc.rest.utils.YamlUtils;
import com.snz1.utils.JsonUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service("jdbcrest::SQLServiceRegistry")
public class SQLServiceRegistryImpl implements SQLServiceRegistry {

  private ResourcePatternResolver resourceLoader = new PathMatchingResourcePatternResolver();

  @Resource
  private RunConfig runConfig;

  @Resource
  private SqlSessionFactory sessionFactory;

  private Map<String, SQLServiceDefinition> sqlServiceDefinitions;

  @PostConstruct
  public void loadSQLServiceDefinitions() {
    String sql_location = runConfig.getSql_location();
    if (StringUtils.isNotBlank(sql_location)) {
      try {
        Validate.isTrue(resourceLoader.getResource(sql_location).exists());
      } catch (Throwable e) {
        log.warn("无法加载SQL服务目录“{}”：{}", sql_location, e.getMessage(), e);
        return;
      }
    }

    while (sql_location.endsWith("/")) {
      sql_location = sql_location.substring(0, sql_location.length() - 1);
    }

    try {
      this.sqlServiceDefinitions = new HashMap<>(this.doLoadSQLServiceDefinitions("", "/services", sql_location));
    } catch (Exception e) {
      if (log.isDebugEnabled()) {
        log.debug("{}", e.getMessage(), e);
      }
      throw new IllegalStateException(e.getMessage(), e);
    }
  }

  protected Map<String, SQLServiceDefinition> doLoadSQLServiceDefinitions(String parent_name, String relative_path, String sql_dir) throws Exception {
    if (log.isDebugEnabled()) {
      log.debug("加载目录 {} 下的SQL文件...", sql_dir);
    }

    String prefix_dir = sql_dir;
    if (StringUtils.startsWith(sql_dir, "classpath:")) {
      prefix_dir = sql_dir.substring("classpath:".length());
    } else if (StringUtils.startsWith(sql_dir, "classpath*:")) {
      prefix_dir = sql_dir.substring("classpath*:".length());
    } else if (StringUtils.startsWith(sql_dir, "file:")) {
      prefix_dir = sql_dir.substring("file:".length());
    }

    while (prefix_dir.startsWith("/")) {
      prefix_dir = prefix_dir.substring(1);
    }

    org.springframework.core.io.Resource[] resources = resourceLoader.getResources(String.format("%s/**/*.sql", sql_dir));
    Map<String, SQLServiceDefinition> defintions = new LinkedHashMap<>();
    for (org.springframework.core.io.Resource resource : resources) {
      String start_uri = resource.getURI().toString();
      int start_x = start_uri.indexOf("!/");
      if (start_x > 0) {
        start_uri = start_uri.substring(start_x + 3);
      } else {
        start_x = start_uri.indexOf(":");
        if (start_x > 0) {
          start_uri = start_uri.substring(start_x + 2);
        }
      }
      String nosuffix_file = start_uri.substring(prefix_dir.length(), start_uri.length() - 4);
      String service_path = String.format("%s/%s", relative_path, nosuffix_file);
      String service_name = String.format("%s%s", parent_name, nosuffix_file);
      if (log.isDebugEnabled()) {
        log.debug("service_path = {}, service_name = {}", service_path, service_name);
      }
      InputStream resource_ism = resource.getInputStream();
      try {
        SQLServiceDefinition def = this.doLoadSQLServiceDefinition(
          service_name, service_path,
          resource.getURI().toString(),
          resource.getInputStream());
        defintions.put(service_path, def);
      } finally {
        IOUtils.closeQuietly(resource_ism);
      }
    }

    return defintions;
  }

  protected SQLServiceDefinition doLoadSQLServiceDefinition(
    String service_name, String service_path,
    String file_path, InputStream sql_file
  ) throws Exception {
    SQLServiceDefinition sql_def = new SQLServiceDefinition();
    sql_def.setService_name(service_name);
    sql_def.setService_path(service_path);
    sql_def.setFile_location(file_path);
    SQLServiceDefinition.SQLFragment sql_frag = null;
    int i = 0;

    for (SQLClauses sql_clauses : JdbcUtils.loadSQLClausesFromFile(sql_file, Constants.SQL_SPLITTER, JsonUtils.JsonCharset)) {
      i = i + 1;
      String sql_line = sql_clauses.getSql().toString();
      String sql_note = sql_clauses.getNote().toString();
      if (log.isDebugEnabled()) {
        log.debug("NOTE:{}", sql_note);
        log.debug("SQL:{}", sql_line);
      }
      if (StringUtils.isBlank(sql_line)) continue;
      // 分析SQL请求
      sql_frag = new SQLServiceDefinition.SQLFragment();
      sql_frag.setFrangment_sql(sql_line);
      sql_frag.setLast_fragment(false);
      // 检查参数
      Map<String, JDBCType> parameter_map = JdbcUtils.parseSQLParamters(sql_line);
      if (parameter_map == null || parameter_map.size() == 0) {
        log.warn("SQL文件{}第[{}]段无参数输入, SQL:\n{}", sql_file.toString(), i, sql_frag.getFrangment_sql());
      }
      // 构建MyBatis映射ID
      sql_frag.setMapped_id(String.format("%s/%d", sql_def.getService_path(), i));

      // 检测查询类型
      SqlCommandType command_type = SqlCommandType.UNKNOWN;
      String lower_sql = StringUtils.stripStart(StringUtils.lowerCase(sql_line), "\r\n ");
      if (StringUtils.startsWith(lower_sql, "select")) {
        command_type = SqlCommandType.SELECT;
      } else if (StringUtils.startsWith(lower_sql, "insert")) {
        command_type = SqlCommandType.INSERT;
      } else if (StringUtils.startsWith(lower_sql, "update")) {
        command_type = SqlCommandType.UPDATE;
      } else if (StringUtils.startsWith(lower_sql, "delete")) {
        command_type = SqlCommandType.DELETE;
      } else if (StringUtils.startsWith(lower_sql, "flush")) {
        command_type = SqlCommandType.FLUSH;
      }

      // 检查SQL命令类型
      if (Objects.equals(SqlCommandType.UNKNOWN, command_type)) {
        log.warn("SQL文件{}第[{}]段SQL有问题, SQL:\n{}", sql_file.toString(), i, sql_frag.getFrangment_sql());
      } else {
        if (log.isDebugEnabled()) {
          log.warn("SQL文件{}第[{}]段SQL:\n{}", sql_file.toString(), i, sql_frag.getFrangment_sql());
        }
        // 分析返回类型
        SQLServiceDefinition.ResultDefinitionYamlWrapper result_wrapper = YamlUtils.fromYaml(
          sql_note, SQLServiceDefinition.ResultDefinitionYamlWrapper.class
        );
        if (result_wrapper == null) {
          result_wrapper = new SQLServiceDefinition.ResultDefinitionYamlWrapper();
        }

        if (result_wrapper.getColumns() != null && result_wrapper.getColumns().length > 0) {
          for (ResultDefinition.ResultColumn cl : result_wrapper.getColumns()) {
            Validate.notBlank(cl.getName(), "字段名不能为空");
            cl.setAlias(null);
            sql_frag.getResult().getColumns().put(cl.getName(), cl);
          }
        }
        sql_frag.getResult().setSignleton(result_wrapper.isSignleton());
        sql_frag.getResult().setColumn_compact(result_wrapper.isColumn_compact());

        if (StringUtils.isBlank(sql_def.getService_title()) && StringUtils.isNotBlank(
          result_wrapper.getTitle()
        )) {
          sql_def.setService_title(result_wrapper.getTitle());
        }
      }

      // 构建MyBatis解析段
      sql_frag.setMapped_statement(JdbcUtils.createMappedStatement(
        sql_frag.getMapped_id(),
        sessionFactory.getConfiguration(),
        command_type, sql_line,
        sql_frag.getResult()
      ));
      if (log.isDebugEnabled()) {
        log.info("SQL Fragment:\n{}", sql_frag);
      }
    }

    if (sql_frag == null) {
      throw new IllegalStateException("SQL文件无任何内容");
    } else {
      sql_frag.setLast_fragment(true);
    }
    sql_def.getSql_fragments().add(sql_frag);
    return sql_def;
  }

  @Override
  public SQLServiceDefinition getService(String service_path) {
    return this.sqlServiceDefinitions.get(service_path);
  }

  @Override
  public Collection<SQLServiceDefinition> getServices() {
    return this.sqlServiceDefinitions.values();
  }

}
