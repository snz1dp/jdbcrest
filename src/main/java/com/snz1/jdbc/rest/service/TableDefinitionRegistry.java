package com.snz1.jdbc.rest.service;

import java.util.List;

import com.snz1.jdbc.rest.data.TableDefinition;

// 数据表定义注册表
public interface TableDefinitionRegistry {

  // 获取数据表定义
  TableDefinition getTableDefinition(String table_name);

  // 是否有数据表定义
  boolean hasTableDefinition(String table_name);

  List<TableDefinition> getTableDefinition();

}
