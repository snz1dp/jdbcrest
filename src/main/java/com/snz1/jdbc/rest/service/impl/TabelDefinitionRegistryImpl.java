package com.snz1.jdbc.rest.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.stereotype.Service;

import com.esotericsoftware.yamlbeans.YamlException;
import com.esotericsoftware.yamlbeans.YamlReader;
import com.snz1.jdbc.rest.data.TableDefinition;
import com.snz1.jdbc.rest.service.TabelDefinitionRegistry;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class TabelDefinitionRegistryImpl implements TabelDefinitionRegistry {
  
  @Value("${app.table.definition:}")
  private String tableDefinitionFile;

  private Map<String, TableDefinition> tableDefinitionMap;

  // 加载表定义配置
  @PostConstruct
  public void loadTableDefinitions() {
    if (StringUtils.isBlank(this.tableDefinitionFile)) {
      log.info("未设置数据表权限配置文件参数");
      return;
    }

    if (log.isInfoEnabled()) {
      log.info("加载数据表权限配置文件{} ...", this.tableDefinitionFile);
    }

    Resource tdf_resource;
    if (StringUtils.startsWithAny(this.tableDefinitionFile, "file://", "http://", "https://")) {
      try {
        tdf_resource = new UrlResource(this.tableDefinitionFile);
      } catch (MalformedURLException e) {
        throw new IllegalArgumentException(String.format("错误的数据表权限配置文件路径: %s", this.tableDefinitionFile));
      }
    } else {
      tdf_resource = new FileSystemResource(this.tableDefinitionFile);
    }

    // 加载资源
    InputStream tdf_ism = null;
		try {
			tdf_ism = tdf_resource.getInputStream();
		} catch (IOException e) {
			throw new IllegalStateException(String.format("无法读取数据表权限配置文件: %s", this.tableDefinitionFile), e);
		}

    // 读取配置
    YamlReader yamlReader = new YamlReader(new InputStreamReader(tdf_ism));
    TableDefinition[] table_definitions;
    try {
      table_definitions = yamlReader.read(TableDefinition[].class);
    } catch (YamlException e) {
			throw new IllegalStateException(String.format("读取数据表权限配置文件%s出错: %s", this.tableDefinitionFile, e.getMessage()), e);
    }

    if (table_definitions == null || table_definitions.length == 0) {
      log.warn("数据表权限配置文件 {} 中无任何配置内容", this.tableDefinitionFile);
      return;
    }

    // 校验定义
    Set<String> table_set = new HashSet<String>(table_definitions.length);
    for (TableDefinition table_def : table_definitions) {
      try {
        Validate.isTrue(
          !table_set.contains(table_def.getTable_name()),
          "数据表重复定义"
        );
        table_def.validate();
      } catch (Throwable e) {
        if (log.isDebugEnabled()) {
          log.debug("{} 定义错误: {}", table_def.getTable_name(), e.getMessage(), e);
        }
        throw new IllegalStateException(String.format(""));
      }
    }

    // 加载定义
    this.tableDefinitionMap = new HashMap<>(table_definitions.length);
    for (TableDefinition table_def : table_definitions) {
      this.tableDefinitionMap.put(tableDefinitionFile, table_def);
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
