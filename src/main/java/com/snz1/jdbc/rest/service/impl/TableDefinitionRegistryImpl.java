package com.snz1.jdbc.rest.service.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.Validate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.esotericsoftware.yamlbeans.YamlException;
import com.esotericsoftware.yamlbeans.YamlReader;
import com.snz1.jdbc.rest.RunConfig;
import com.snz1.jdbc.rest.data.TableDefinition;
import com.snz1.jdbc.rest.service.TableDefinitionRegistry;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class TableDefinitionRegistryImpl implements TableDefinitionRegistry {

  @Autowired
  private RunConfig runConfig;

  private Map<String, TableDefinition> tableDefinitionMap = Collections.emptyMap();

  // 加载表定义配置
  @PostConstruct
  public void loadTableDefinitions() {
    File deffile = runConfig.getTable_definition_file();
    if (deffile == null) {
      if (log.isDebugEnabled()) {
        log.debug("未设置数据表配置文件参数, 忽略数据表配置加载");
      }
      return;
    }

    if (log.isInfoEnabled()) {
      log.info("加载数据表配置文件{} ...", deffile);
    }

    // 加载资源
    FileInputStream tdf_ism;
		try {
			tdf_ism = new FileInputStream(deffile);
		} catch (IOException e) {
			throw new IllegalStateException(String.format("无法读取数据表配置文件: %s", deffile), e);
		}

    // 读取配置
    YamlReader yamlReader = new YamlReader(new InputStreamReader(tdf_ism));
    TableDefinition[] table_definitions;
    try {
      table_definitions = yamlReader.read(TableDefinition[].class);
    } catch (YamlException e) {
			throw new IllegalStateException(String.format("读取数据表配置文件%s出错: %s", deffile, e.getMessage()), e);
    }

    if (table_definitions == null || table_definitions.length == 0) {
      log.warn("数据表配置文件 {} 中无任何配置内容", deffile);
      return;
    }

    // 校验定义
    Set<String> table_set = new HashSet<String>(table_definitions.length);
    for (TableDefinition table_def : table_definitions) {
      try {
        Validate.isTrue(
          !table_set.contains(table_def.getName()),
          "数据表重复定义"
        );
        table_def.validate();
      } catch (Throwable e) {
        if (log.isDebugEnabled()) {
          log.debug("{} 定义错误: {}", table_def.getName(), e.getMessage(), e);
        }
        throw new IllegalStateException(String.format(""));
      }
    }

    // 加载定义
    this.tableDefinitionMap = new HashMap<>(table_definitions.length);
    for (TableDefinition table_def : table_definitions) {
      this.tableDefinitionMap.put(table_def.getName(), table_def);
    }

  }

  @Override
  public TableDefinition getTableDefinition(String table_name) {
    return this.tableDefinitionMap.get(table_name);
  }

  @Override
  public boolean hasTableDefinition(String table_name) {
    return this.tableDefinitionMap.containsKey(table_name);
  }

}
