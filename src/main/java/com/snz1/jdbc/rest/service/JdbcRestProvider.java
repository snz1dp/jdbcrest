package com.snz1.jdbc.rest.service;

import java.sql.SQLException;
import java.util.List;

import com.snz1.jdbc.rest.data.JdbcInsertRequest;
import com.snz1.jdbc.rest.data.JdbcMetaData;
import com.snz1.jdbc.rest.data.JdbcQueryRequest;
import com.snz1.jdbc.rest.data.JdbcQueryResponse;
import com.snz1.jdbc.rest.data.TableMeta;
import com.snz1.jdbc.rest.data.TableQueryRequest;

public interface JdbcRestProvider {

  // 获取元信息
  JdbcMetaData getMetaData() throws SQLException;

  // 获取Schema
  JdbcQueryResponse<List<Object>> getSchemas() throws SQLException;

  // 获取目录
  JdbcQueryResponse<List<Object>> getCatalogs() throws SQLException;

  // 获取表主键
  Object getTablePrimaryKey(String table_name) throws SQLException;

  // 测试表是否存在
  boolean testTableExisted(String table_name, String ...types);

  // 获取表
  JdbcQueryResponse<List<Object>> getTables(
    JdbcQueryRequest.ResultMeta return_meta,
    String catalog, String schemaPattern,
    String tableNamePattern, String... types
  ) throws SQLException;

  // 查询表
  public JdbcQueryResponse<?> queryPageResult(
    TableQueryRequest table_query
  ) throws SQLException;

  // 查询统计信息
  public long queryAllCountResult(
    TableQueryRequest table_query
  ) throws SQLException;

  // 查询统计信息
  public JdbcQueryResponse<?> queryGroupCountResult(
    TableQueryRequest table_query
  ) throws SQLException;

  // 元信息
  public TableMeta queryResultMeta(
    TableQueryRequest table_query
  ) throws SQLException;

  // 查询分组信息
  public JdbcQueryResponse<?> queryGroupResult(
    TableQueryRequest table_query
  ) throws SQLException;

  // 查询单个应答
  public JdbcQueryResponse<?> querySignletonResult(
    TableQueryRequest table_query
  ) throws SQLException;

  // 插入表数据
  public Object insertTableData(
    JdbcInsertRequest insert_request
  ) throws SQLException;

}
