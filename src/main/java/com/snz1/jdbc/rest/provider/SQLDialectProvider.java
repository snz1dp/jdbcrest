package com.snz1.jdbc.rest.provider;

import java.sql.SQLException;

import com.snz1.jdbc.rest.data.JdbcQueryStatement;
import com.snz1.jdbc.rest.data.ManipulationRequest;
import com.snz1.jdbc.rest.service.JdbcTypeConverterFactory;
import com.snz1.jdbc.rest.data.JdbcQueryRequest;

// SQL方言实现
public interface SQLDialectProvider {

  // 是否检查数据表存在
  boolean checkTableExisted();

  // 是否支持Schema
  boolean supportSchemas();

  // 是否允许用count(*)
  boolean supportCountAnyColumns();

  // 获取类型转换工厂
  JdbcTypeConverterFactory getTypeConverterFactory();

  // 准备数据查询
  JdbcQueryStatement prepareQuerySelect(JdbcQueryRequest table_query);

  // 获取查询的合计
  JdbcQueryStatement prepareQueryCount(JdbcQueryRequest table_query);

  // 准备分页查询
  JdbcQueryStatement preparePageSelect(
    JdbcQueryRequest table_query
  ) throws SQLException;

  // 准备插入数据
  JdbcQueryStatement prepareDataInsert(
    ManipulationRequest insert_request
  );

  // 准备更新数据
  JdbcQueryStatement prepareDataUpdate(
    ManipulationRequest update_request
  );

  // 准备删除数据
  JdbcQueryStatement prepareDataDelete(
    ManipulationRequest delete_request
  );

}
