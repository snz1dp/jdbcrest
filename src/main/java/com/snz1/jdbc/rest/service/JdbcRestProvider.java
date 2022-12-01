package com.snz1.jdbc.rest.service;

import java.sql.SQLException;
import java.util.List;

import com.snz1.jdbc.rest.data.JdbcMetaData;
import com.snz1.jdbc.rest.data.JdbcQueryRequest;
import com.snz1.jdbc.rest.data.JdbcQueryResponse;
import com.snz1.jdbc.rest.data.TableQueryRequest;

import gateway.api.Page;

public interface JdbcRestProvider {

  // 获取元信息
  JdbcMetaData getMetaData() throws SQLException;

  // 获取Schema
  JdbcQueryResponse<List<Object>> getSchemas() throws SQLException;

  // 获取目录
  JdbcQueryResponse<List<Object>> getCatalogs() throws SQLException;

  // 获取表
  JdbcQueryResponse<List<Object>> getTables(
    JdbcQueryRequest.ResultMeta return_meta,
    String catalog, String schemaPattern,
    String tableNamePattern, String... types
  ) throws SQLException;

  // 查询表
  public JdbcQueryResponse<Page<Object>> queryPageResult(
    TableQueryRequest table_query
  ) throws SQLException;

  // 查询统计信息
  public int queryAllCountResult(
    TableQueryRequest table_query
  );

  // 查询统计信息
  public JdbcQueryResponse<Page<Object>> queryGroupCountResult(
    TableQueryRequest table_query
  );

  // 元信息
  public JdbcQueryResponse.ResultMeta queryResultMeta(
    TableQueryRequest table_query
  ) throws SQLException;

}
