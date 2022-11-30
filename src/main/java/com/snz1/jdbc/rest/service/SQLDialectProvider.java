package com.snz1.jdbc.rest.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.snz1.jdbc.rest.data.TableQueryRequest;

public interface SQLDialectProvider {

  String getName();

  PreparedStatement prepareZeroSelect(
    Connection conn,
    TableQueryRequest table_query
  ) throws SQLException;

  PreparedStatement prepareTableSelect(
    Connection conn,
    TableQueryRequest table_query
  ) throws SQLException;

}
