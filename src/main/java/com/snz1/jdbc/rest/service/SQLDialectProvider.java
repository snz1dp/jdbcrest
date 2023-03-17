package com.snz1.jdbc.rest.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.snz1.jdbc.rest.data.JdbcQueryStatement;
import com.snz1.jdbc.rest.data.ManipulationRequest;
import com.snz1.jdbc.rest.data.JdbcQueryRequest;

// SQL方言实现
public interface SQLDialectProvider {

  // 数据库ID
  String getId();

  // 是否检查数据表存在
  boolean checkTableExisted();

  // 获取查询的合计
  JdbcQueryStatement prepareQueryCount(JdbcQueryRequest table_query);

  // 准备无行查询
  @Deprecated
  PreparedStatement prepareNoRowSelect(
    Connection conn,
    JdbcQueryRequest table_query
  ) throws SQLException;

  // 准备分页查询
  PreparedStatement preparePageSelect(
    Connection conn,
    JdbcQueryRequest table_query
  ) throws SQLException;

  // 准备插入数据
  PreparedStatement prepareDataInsert(
    Connection conn,
    ManipulationRequest insert_request
  ) throws SQLException;

  // 准备更新数据
  PreparedStatement prepareDataUpdate(
    Connection conn,
    ManipulationRequest update_request
  ) throws SQLException;

  // 准备删除数据
  PreparedStatement prepareDataDelete(
    Connection conn,
    ManipulationRequest delete_request
  ) throws SQLException;

}
