package com.snz1.jdbc.rest.service.impl;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.sql.JDBCType;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.ibatis.mapping.SqlCommandType;
import org.apache.ibatis.session.SqlSessionFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.UrlResource;
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
@Service
public class SQLServiceRegistryImpl implements SQLServiceRegistry {
    
  @Resource
  private RunConfig runConfig;

  @Resource
  private SqlSessionFactory sessionFactory;

  private Map<String, SQLServiceDefinition> sqlServiceDefinitions;

  @PostConstruct
  public void loadSQLServiceDefinitions() {
    String sql_location = runConfig.getSql_location();
    if (StringUtils.isBlank(sql_location)) return;
    File sql_directory;
    try {
      sql_directory = new UrlResource(sql_location).getFile();
    } catch (IOException e) {
      if (e instanceof MalformedURLException) {
          try {
            sql_directory = new ClassPathResource(sql_location).getFile();
          } catch (IOException ex) {
            log.warn("无法获取SQL服务目录信息信息, Path={}, 错误信息: {}", sql_location, e.getMessage(), e);
            return;
          }
      } else {
        log.warn("无法获取SQL服务目录信息信息, Path={}, 错误信息: {}", sql_location, e.getMessage(), e);
        return;
      }
    }
    Validate.isTrue(sql_directory.isDirectory(), "%s不是一个有效的目录", sql_location);
    try {
      this.doLoadSQLServiceDefinitions("/services", sql_directory);
    } catch (Exception e) {
      log.warn("加载SQL服务失败: {}", e.getMessage(), e);
    }
  }

  protected void doLoadSQLServiceDefinitions(String relative_path, File sql_dir) throws Exception {
    if (log.isDebugEnabled()) {
      log.debug("加载目录 {} 下的SQL文件...", sql_dir);
    }
    
    Map<String, SQLServiceDefinition> defintions = new LinkedHashMap<>();
    for (File file : sql_dir.listFiles()) {
      if (file.isDirectory()) {
        this.doLoadSQLServiceDefinitions(String.format("%s/%s", relative_path, file.getName()), file);
      }
      if (!StringUtils.endsWith(StringUtils.lowerCase(file.getName()), ".sql")) continue;
      if (log.isDebugEnabled()) {
        log.debug("加载SQL文件 {} ...", file.toString());
      }
      try {
        String service_path = String.format("%s/%s", relative_path, file.getName().substring(0, file.getName().length() - 4));
        if (StringUtils.endsWith(service_path, "/")) {
          service_path = service_path.substring(0, service_path.length() - 1);
        }
        SQLServiceDefinition def = this.doSQLServiceDefinition(service_path, file);
        defintions.put(service_path, def);
      } catch (Throwable e) {
        throw new IllegalStateException(String.format("加载SQL文件%s失败: %s", file.toString(), e.getMessage()), e);
      }
    }
    this.sqlServiceDefinitions = new HashMap<>(defintions);
  }

  protected SQLServiceDefinition doSQLServiceDefinition(String service_path, File sql_file) throws Exception {
    SQLServiceDefinition sql_def = new SQLServiceDefinition();
    sql_def.setService_path(service_path);
    sql_def.setFile_location(sql_file.getAbsolutePath());
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

        }
        
        // 分析返回类型
        SQLServiceDefinition.ResultDefinitionYamlWrapper result_wrapper = YamlUtils.fromYaml(
          sql_note, SQLServiceDefinition.ResultDefinitionYamlWrapper.class
        );
        if (result_wrapper.getColumns() != null && result_wrapper.getColumns().length > 0) {
          for (ResultDefinition.ResultColumn cl : result_wrapper.getColumns()) {
            Validate.notBlank(cl.getName(), "字段名不能为空");
            cl.setAlias(null);
            sql_frag.getResult().getColumns().put(cl.getName(), cl);
          }
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

}
