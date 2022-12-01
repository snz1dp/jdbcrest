package com.snz1.jdbc.rest.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.snz1.jdbc.rest.data.JdbcQuery;
import com.snz1.jdbc.rest.data.TableQueryRequest;

// SQL方言实现
public interface SQLDialectProvider {

  // 数据库名称
  String getName();

  // 获取查询的合计
  JdbcQuery prepareQueryCount(TableQueryRequest table_query);

  // 无行查询
  PreparedStatement prepareNoRowSelect(
    Connection conn,
    TableQueryRequest table_query
  ) throws SQLException;

  // 分页查询
  PreparedStatement preparePageSelect(
    Connection conn,
    TableQueryRequest table_query
  ) throws SQLException;

}
