package com.snz1.jdbc.rest.service.impl;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.UrlResource;
import org.springframework.stereotype.Service;

import com.snz1.jdbc.rest.Constants;
import com.snz1.jdbc.rest.RunConfig;
import com.snz1.jdbc.rest.data.SQLServiceDefinition;
import com.snz1.jdbc.rest.service.SQLServiceRegistry;
import com.snz1.jdbc.rest.utils.JdbcUtils;
import com.snz1.utils.JsonUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class SQLServiceRegistryImpl implements SQLServiceRegistry {
    
  @Resource
  private RunConfig runConfig;

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
      this.doLoadSQLServiceDefinitions("", sql_directory);
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
        String service_path = file.getName().substring(0, file.getName().length() - 4);
        SQLServiceDefinition def = this.doSQLServiceDefinition(file);
        def.setService_path(service_path);
        defintions.put(service_path, def);
      } catch (Throwable e) {
        throw new IllegalStateException(String.format("加载SQL文件%s失败: %s", file.toString(), e.getMessage()), e);
      }
    }
    this.sqlServiceDefinitions = new HashMap<>(defintions);
  }

  protected SQLServiceDefinition doSQLServiceDefinition(File sql_file) throws Exception {
    SQLServiceDefinition sql_def = new SQLServiceDefinition();
    sql_def.setFile_location(sql_file.getAbsolutePath());
    SQLServiceDefinition.SQLFragment sql_frag = null;
    int i = 0;

    for (String sql_line : JdbcUtils.loadSQLBatchFromURL(sql_file, Constants.SQL_SPLITTER, JsonUtils.JsonCharset)) {
      i = i + 1;
      if (StringUtils.isBlank(sql_line)) continue;
      sql_frag = new SQLServiceDefinition.SQLFragment();
      sql_frag.setFrangment_sql(sql_line);
      sql_frag.setLast_fragment(false);
      JdbcUtils.parseSQLParamters(sql_line, sql_frag.getParameter_map());
      if (!sql_frag.hasParameters()) {
        log.warn("SQL文件{}第[{}]段无参数输入", sql_file.toString(), i);
      }
    }

    if (sql_frag == null) {
      throw new IllegalStateException("SQL文件无任何内容");
    } else {
      sql_frag.setLast_fragment(true);
    }
    return sql_def;
  }

}
