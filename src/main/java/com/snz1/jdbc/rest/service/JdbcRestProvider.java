package com.snz1.jdbc.rest.service;

import java.sql.SQLException;
import java.util.List;

import com.snz1.jdbc.rest.data.JdbcDMLRequest;
import com.snz1.jdbc.rest.data.JdbcDMLResponse;
import com.snz1.jdbc.rest.data.JdbcMetaData;
import com.snz1.jdbc.rest.data.JdbcQueryRequest;
import com.snz1.jdbc.rest.data.JdbcQueryResponse;
import com.snz1.jdbc.rest.data.ManipulationRequest;
import com.snz1.jdbc.rest.data.Page;
import com.snz1.jdbc.rest.data.ResultDefinition;
import com.snz1.jdbc.rest.data.SQLServiceRequest;
import com.snz1.jdbc.rest.data.TableMeta;

// JDBC转Rest接口提供器
public interface JdbcRestProvider {

  // 获取元信息
  JdbcMetaData getMetaData() throws SQLException;

  // 获取Schema
  JdbcQueryResponse<List<Object>> getSchemas() throws SQLException;

  // 获取目录
  JdbcQueryResponse<List<Object>> getCatalogs() throws SQLException;

  // 获取表
  JdbcQueryResponse<Page<Object>> getTables(
    ResultDefinition return_meta,
    String catalog, String schemaPattern,
    String tableNamePattern, 
    Long offset, Long limit, String... types
  ) throws SQLException;

  // 查询表
  public JdbcQueryResponse<?> queryPageResult(
    JdbcQueryRequest table_query
  ) throws SQLException;

  // 查询统计信息
  public JdbcQueryResponse<Long> queryAllCountResult(
    JdbcQueryRequest table_query
  ) throws SQLException;

  // 查询统计信息
  public JdbcQueryResponse<?> queryGroupCountResult(
    JdbcQueryRequest table_query
  ) throws SQLException;

  // 元信息
  public TableMeta queryResultMeta(
    JdbcQueryRequest table_query
  ) throws SQLException;

  // 查询分组信息
  public JdbcQueryResponse<?> queryGroupResult(
    JdbcQueryRequest table_query
  ) throws SQLException;

  // 查询单个应答
  public JdbcQueryResponse<?> querySignletonResult(
    JdbcQueryRequest table_query
  ) throws SQLException;

  // 插入表数据
  public Object insertTableData(
    ManipulationRequest insert_request
  ) throws SQLException;

  // 更新表数据
  public Object updateTableData(
    ManipulationRequest update_request
  ) throws SQLException;

  // 删除表数据
  public Object deleteTableData(
    ManipulationRequest delete_request
  ) throws SQLException;

  // 执行DML请求
  public List<JdbcDMLResponse> executeDMLRequest(
    JdbcDMLRequest[] request
  ) throws SQLException;

  // 执行SQL服务
  <T> T executeSQLService(
    SQLServiceRequest sql_request
  );

  // 清理表数据缓存
  void clearTableCaches(String table_name);

  // 清理表数据缓存
  void clearTableCaches();

  // 清理元信息缓存
  void clearMetaCaches();

  // 清理SQL服务缓存
  void clearServiceCaches(String service_path);

  // 清理全部SQL服务缓存
  void clearServiceCaches();

}
