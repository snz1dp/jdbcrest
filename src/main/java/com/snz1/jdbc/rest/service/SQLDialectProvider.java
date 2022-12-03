package com.snz1.jdbc.rest.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.snz1.jdbc.rest.data.JdbcQuery;
import com.snz1.jdbc.rest.data.ManipulationRequest;
import com.snz1.jdbc.rest.data.TableQueryRequest;

// SQL方言实现
public interface SQLDialectProvider {

  // 数据库名称
  String getName();

  // 获取查询的合计
  JdbcQuery prepareQueryCount(TableQueryRequest table_query);

  // 准备无行查询
  PreparedStatement prepareNoRowSelect(
    Connection conn,
    TableQueryRequest table_query
  ) throws SQLException;

  // 准备分页查询
  PreparedStatement preparePageSelect(
    Connection conn,
    TableQueryRequest table_query
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

}
