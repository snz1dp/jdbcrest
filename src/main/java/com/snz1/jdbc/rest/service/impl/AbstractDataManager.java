package com.snz1.jdbc.rest.service.impl;

import java.lang.reflect.Array;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;

import javax.annotation.Resource;

import org.apache.commons.lang3.Validate;

import com.snz1.jdbc.rest.data.ConditionOperation;
import com.snz1.jdbc.rest.data.JdbcQueryRequest;
import com.snz1.jdbc.rest.data.JdbcQueryResponse;
import com.snz1.jdbc.rest.data.ManipulationRequest;
import com.snz1.jdbc.rest.data.Page;
import com.snz1.jdbc.rest.data.SQLServiceDefinition;
import com.snz1.jdbc.rest.data.SQLServiceRequest;
import com.snz1.jdbc.rest.data.TableColumn;
import com.snz1.jdbc.rest.data.TableMeta;
import com.snz1.jdbc.rest.data.WhereCloumn;
import com.snz1.jdbc.rest.service.JdbcRestProvider;
import com.snz1.jdbc.rest.service.SQLServiceRegistry;
import com.snz1.jdbc.rest.utils.ObjectUtils;

// 抽象的数据管理实现类
public abstract class AbstractDataManager {
  
  @Resource
  private JdbcRestProvider restProvider;

  @Resource
  private SQLServiceRegistry serviceRegistry;

  // 获取JDBC REST提供器
  protected JdbcRestProvider getRestProvider() {
    return restProvider;
  }

  // 获取服务注册表
  protected SQLServiceRegistry getServiceRegistry() {
    return serviceRegistry;
  }

  // 获取表元信息
  protected TableMeta getTableMeta(String table_name) throws SQLException {
    return this.getRestProvider().queryResultMeta(JdbcQueryRequest.of(table_name));
  }

  //构造SQL服务请求
  protected SQLServiceRequest buildServiceRequest(String service_path, Object input_data) throws SQLException {
    return this.buildServiceRequest(service_path, input_data, null);
  }

  // 构造SQL服务请求
  protected SQLServiceRequest buildServiceRequest(String service_path, Object input_data, Class<?> clazz) throws SQLException {
    SQLServiceDefinition sql_service = this.getServiceRegistry().getService(service_path);
    Validate.notNull(sql_service, "SQL服务不存在");
    SQLServiceRequest sql_request = SQLServiceRequest.of(sql_service);
    sql_request.setResult_class(clazz);
    sql_request.setInput_data(input_data);
    return sql_request;
  }

  // 创建数据对象
  protected <T> Object createDataObject(TableMeta result_meta, T input_data) throws SQLException {
    ManipulationRequest insert_request = new ManipulationRequest();
    insert_request.setTable_name(result_meta.getTable_name());
    insert_request.copyTableMeta(result_meta);
    insert_request.setInput_data(ObjectUtils.objectToMap(input_data));
    return restProvider.insertTableData(insert_request);
  }

  protected <T> T getDataObject(TableMeta table_meta, Object id) throws SQLException {
    return this.getDataObject(table_meta, id, null);
  }

  protected <T> T getDataObject(TableMeta table_meta, Object id, Class<T> entity_class) throws SQLException {
    return this.getDataObject(table_meta, id, null, entity_class);
  }

  @SuppressWarnings("unchecked")
  protected <T> T getDataObject(TableMeta table_meta, Object id, Object primary_col, Class<T> entity_class) throws SQLException {
    // 获取表元信息
    JdbcQueryRequest table_query = new JdbcQueryRequest();
    table_query.setTable_name(table_meta.getTable_name());
    table_query.copyTableMeta(table_meta);

    if (entity_class != null) {
      table_query.getResult().setEntity_class(entity_class);
    }

    WhereCloumn where_col = null;
    if (primary_col == null) {
      primary_col = table_meta.getPrimary_key();
    }

    if (primary_col instanceof String) {
      where_col = WhereCloumn.of((String)primary_col);
      TableColumn col = table_query.getTable_meta().findColumn(where_col.getColumn());
      if (col != null) {
        where_col.setType(col.getJdbc_type());
      }
      where_col.addCondition(ConditionOperation.$eq, id);
      table_query.getWhere().add(where_col);
    } else if (primary_col.getClass().isArray()) {
      for (int i = 0; i < Array.getLength(primary_col); i++) {
        Object col = Array.get(primary_col, i);
        where_col = WhereCloumn.of((String)col);
        TableColumn col_meta = table_query.getTable_meta().findColumn(where_col.getColumn());
        if (col_meta != null) {
          where_col.setType(col_meta.getJdbc_type());
        }
        where_col.addCondition(ConditionOperation.$eq, Array.get(id, i));
        table_query.getWhere().add(where_col);
      }
    } else if (primary_col instanceof Collection) {
      Iterator<?> idata = ((Collection<?>)id).iterator();
      for (Object col : (Collection<?>)primary_col) {
        where_col = WhereCloumn.of((String)col);
        TableColumn col_meta = table_query.getTable_meta().findColumn(where_col.getColumn());
        if (col_meta != null) {
          where_col.setType(col_meta.getJdbc_type());
        }
        where_col.addCondition(ConditionOperation.$eq, idata.next());
        table_query.getWhere().add(where_col);
      }
    } else {
      throw new SQLException("主键类型不支持");
    }

    table_query.getResult().setSignleton(true);
    JdbcQueryResponse<?> ret = restProvider.querySignletonResult(table_query);
    return (T)ret.getData();
  }

  @SuppressWarnings("unchecked")
  protected <T> Page<T> getDataObjectPage(JdbcQueryRequest table_query) throws SQLException {
    // 分页查询
    JdbcQueryResponse<?> ret = restProvider.queryPageResult(table_query);
    return (Page<T>)ret.getData();
  }

  /**
   * 统计数据
   * @param table_query 表查询请求
   * @param count_key 统计用的字段名
   * @return
   * @throws SQLException
   */
  protected long countDataObject(JdbcQueryRequest table_query, String count_key) throws SQLException {
    table_query.getSelect().setCount(count_key);
    table_query.getResult().setColumn_compact(true);
    table_query.getResult().setSignleton(true);
    return (Long)restProvider.querySignletonResult(table_query).data;
  }

  /**
   * 创建数据操作请求
   * @param result_meta 表定义
   * @param input_key 主键值
   * @param input_data 输入参数
   * @param patch 是否补丁
   * @return
   * @throws SQLException
   */
  protected ManipulationRequest createDataObjectManipulationRequest(
    TableMeta result_meta,
    Object input_key,
    Object input_data,
    Boolean patch
  ) throws SQLException {
    // 构建操作请求
    ManipulationRequest update_request = new ManipulationRequest();
    // 获取表元信息
    update_request.setTable_name(result_meta.getTable_name());
    update_request.copyTableMeta(result_meta);
    if (input_data != null) {
      update_request.setInput_data(ObjectUtils.objectToMap(input_data, patch));
    }
    update_request.setInput_key(input_key);
    return update_request;
  }

  /**
   * 更新数据对象
   * @param result_meta 表定义
   * @param input_key 主键值
   * @param input_data 更新数据
   * @throws SQLException
   */
  protected void updateDataObject(TableMeta result_meta, Object input_key, Object input_data) throws SQLException {
    this.updateDataObject(result_meta, input_key, input_data, false);
  }

  /**
   * 更新数据对象
   * @param result_meta 表定义
   * @param input_key 主键值
   * @param input_data 更新数据
   * @param patch 是否补丁修改
   * @throws SQLException
   */
  protected void updateDataObject(TableMeta result_meta, Object input_key, Object input_data, boolean patch) throws SQLException {
    this.updateDataObject(result_meta, input_key, null, input_data, patch);
  }

  protected void updateDataObject(TableMeta result_meta, Object input_key, Object primary_col, Object input_data, boolean patch) throws SQLException {
    ManipulationRequest update_request = this.createDataObjectManipulationRequest(
      result_meta, input_key, input_data, patch
    );
    update_request.setPatch_update(patch);

    WhereCloumn where_col = null;
    if (primary_col == null) {
      primary_col = result_meta.getPrimary_key();
    }

    if (primary_col instanceof String) {
      where_col = WhereCloumn.of((String)primary_col);
      TableColumn col = result_meta.findColumn(where_col.getColumn());
      if (col != null) {
        where_col.setType(col.getJdbc_type());
      }
      where_col.addCondition(ConditionOperation.$eq, input_key);
      update_request.getWhere().add(where_col);
    } else if (primary_col.getClass().isArray()) {
      for (int i = 0; i < Array.getLength(primary_col); i++) {
        Object col = Array.get(primary_col, i);
        where_col = WhereCloumn.of((String)col);
        TableColumn col_meta = result_meta.findColumn(where_col.getColumn());
        if (col_meta != null) {
          where_col.setType(col_meta.getJdbc_type());
        }
        where_col.addCondition(ConditionOperation.$eq, Array.get(input_key, i));
        update_request.getWhere().add(where_col);
      }
    } else if (primary_col instanceof Collection) {
      Iterator<?> idata = ((Collection<?>)input_key).iterator();
      for (Object col : (Collection<?>)primary_col) {
        where_col = WhereCloumn.of((String)col);
        TableColumn col_meta = result_meta.findColumn(where_col.getColumn());
        if (col_meta != null) {
          where_col.setType(col_meta.getJdbc_type());
        }
        where_col.addCondition(ConditionOperation.$eq, idata.next());
        update_request.getWhere().add(where_col);
      }
    } else {
      throw new SQLException("主键类型不支持");
    }

    restProvider.updateTableData(update_request);
  }

  /**
   * 删除数据对象
   * @param result_meta 表定义
   * @param input_key 主键值
   * @throws SQLException
   */
  protected void deleteDataObject(TableMeta result_meta, Object input_key) throws SQLException {
    this.deleteDataObject(result_meta, input_key, null);
  }

  protected void deleteDataObject(TableMeta result_meta, Object input_key, Object primary_col) throws SQLException {
    ManipulationRequest delete_request = this.createDataObjectManipulationRequest(
      result_meta, input_key, null, false);
    WhereCloumn where_col = null;
    if (primary_col == null) {
      primary_col = result_meta.getPrimary_key();
    }

    if (primary_col instanceof String) {
      where_col = WhereCloumn.of((String)primary_col);
      TableColumn col = result_meta.findColumn(where_col.getColumn());
      if (col != null) {
        where_col.setType(col.getJdbc_type());
      }
      where_col.addCondition(ConditionOperation.$eq, input_key);
      delete_request.getWhere().add(where_col);
    } else if (primary_col.getClass().isArray()) {
      for (int i = 0; i < Array.getLength(primary_col); i++) {
        Object col = Array.get(primary_col, i);
        where_col = WhereCloumn.of((String)col);
        TableColumn col_meta = result_meta.findColumn(where_col.getColumn());
        if (col_meta != null) {
          where_col.setType(col_meta.getJdbc_type());
        }
        where_col.addCondition(ConditionOperation.$eq, Array.get(input_key, i));
        delete_request.getWhere().add(where_col);
      }
    } else if (primary_col instanceof Collection) {
      Iterator<?> idata = ((Collection<?>)input_key).iterator();
      for (Object col : (Collection<?>)primary_col) {
        where_col = WhereCloumn.of((String)col);
        TableColumn col_meta = result_meta.findColumn(where_col.getColumn());
        if (col_meta != null) {
          where_col.setType(col_meta.getJdbc_type());
        }
        where_col.addCondition(ConditionOperation.$eq, idata.next());
        delete_request.getWhere().add(where_col);
      }
    } else {
      throw new SQLException("主键类型不支持");
    }
    restProvider.deleteTableData(delete_request);
  }

}
