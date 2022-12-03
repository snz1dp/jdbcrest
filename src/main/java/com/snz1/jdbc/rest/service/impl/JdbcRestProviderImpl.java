package com.snz1.jdbc.rest.service.impl;

import java.io.ByteArrayInputStream;
import java.lang.reflect.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Resource;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.Validate;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;

import com.snz1.jdbc.rest.Constants;
import com.snz1.jdbc.rest.data.ManipulationRequest;
import com.snz1.jdbc.rest.data.JdbcMetaData;
import com.snz1.jdbc.rest.data.JdbcQuery;
import com.snz1.jdbc.rest.data.JdbcQueryRequest;
import com.snz1.jdbc.rest.data.JdbcQueryResponse;
import com.snz1.jdbc.rest.data.Page;
import com.snz1.jdbc.rest.data.TableMeta;
import com.snz1.jdbc.rest.data.TableColumn;
import com.snz1.jdbc.rest.data.TableIndex;
import com.snz1.jdbc.rest.data.TableQueryRequest;
import com.snz1.jdbc.rest.service.JdbcRestProvider;
import com.snz1.jdbc.rest.service.SQLDialectProvider;
import com.snz1.jdbc.rest.utils.JdbcUtils;
import com.snz1.utils.JsonUtils;

import gateway.api.NotFoundException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class JdbcRestProviderImpl implements JdbcRestProvider {

  private JdbcMetaData jdbcMetaData;

  @Resource
  private JdbcTemplate jdbcTemplate;

  @Resource
  private Map<String, SQLDialectProvider> sqlDialectProviders = new HashMap<>();

  @EventListener(ContextRefreshedEvent.class)
  public void loadSQLDialectProviders(ContextRefreshedEvent event) {
    event.getApplicationContext().getBeansOfType(SQLDialectProvider.class).forEach((k, v) -> {
      sqlDialectProviders.put(v.getName(), v);
    });
  }

  // 执行获取结果集
  @SuppressWarnings("null")
  protected JdbcQueryResponse<List<Object>> doFetchResultSet(
    ResultSet rs, JdbcQueryRequest.ResultMeta return_meta,
    Object primary_key, List<TableIndex> unique_index
  ) throws SQLException {
    boolean onepack = true; 
    boolean meta = false;
    boolean objlist = false;

    if (return_meta != null) {
      meta = return_meta.isContain_meta();
      onepack = return_meta.isColumn_compact();
      objlist = JdbcQueryRequest.ResultMeta.ResultObjectStruct.list.equals(return_meta.getRow_struct());
    }

    TableMeta result_meta = TableMeta.of(rs.getMetaData(), return_meta, primary_key, unique_index);
    List<Object> rows = new LinkedList<>();

    while(rs.next()) {
      List<Object> row_list = null;
      Map<String, Object> row_map = null;
      Object row_data = null;
      if (onepack && result_meta.getColumn_count() == 1) {
        row_data = null;
      } else if (objlist) {
        row_list = new LinkedList<>();
      } else {
        row_map = new LinkedHashMap<>();
      }
      for (TableColumn col_item : result_meta.getColumns()) {
        String col_name = col_item.getName();
        Object col_obj = JdbcUtils.getResultSetValue(rs, col_item.getIndex() + 1);
        if (col_obj != null) {
          JdbcQueryRequest.ResultMeta.ResultColumn coldef = return_meta != null ? return_meta.getColumns().get(col_item.getName()) : null;
          if (coldef != null && !Objects.equals(coldef.getType(), JdbcQueryRequest.ResultMeta.ColumnType.raw)) {
            if (Objects.equals(coldef.getType(), JdbcQueryRequest.ResultMeta.ColumnType.list)) {
              if (col_item.getJdbc_type() == JDBCType.BLOB) {
                col_obj = JsonUtils.fromJson(new ByteArrayInputStream((byte[])col_obj), List.class);
              } else {
                col_obj = JsonUtils.fromJson(col_obj.toString(), List.class);
              }
            } else if (Objects.equals(coldef.getType(), JdbcQueryRequest.ResultMeta.ColumnType.map)) {
              if (col_item.getJdbc_type() == JDBCType.BLOB) {
                col_obj = JsonUtils.fromJson(new ByteArrayInputStream((byte[])col_obj), Map.class);
              } else {
                col_obj = JsonUtils.fromJson(col_obj.toString(), Map.class);
              }
            } else {
              if (col_item.getJdbc_type() == JDBCType.BLOB) {
                col_obj = Base64.encodeBase64String((byte[])col_obj);
              } else {
                col_obj = Base64.encodeBase64String(col_obj.toString().getBytes());
              }
            }
          } else if (col_item.getJdbc_type() == JDBCType.BLOB) {
            col_obj = Base64.encodeBase64String((byte[])col_obj);
          }
        }
        if (onepack && result_meta.getColumn_count() == 1) {
          row_data = col_obj;
        } else if (objlist) {
          row_list.add(col_obj);
        } else if (col_obj != null) {
          row_map.put(col_name, col_obj);
        }
      }

      if (onepack && result_meta.getColumn_count() == 1) {
        rows.add(row_data);
      } else if (objlist) {
        rows.add(row_list);
      } else {
        rows.add(row_map);
      }
    }
    JdbcQueryResponse<List<Object>> ret = new JdbcQueryResponse<>();
    if (meta) {
      ret.setMeta(result_meta);
    }
    ret.setData(rows);
    return ret;
  }

  // 获取Schemas
  public JdbcQueryResponse<List<Object>> getSchemas() throws SQLException {
    return jdbcTemplate.execute(new ConnectionCallback<JdbcQueryResponse<List<Object>>>() {

      @Override
      @Nullable
      public JdbcQueryResponse<List<Object>> doInConnection(Connection conn) throws SQLException, DataAccessException {
        DatabaseMetaData table_meta =  conn.getMetaData();
        ResultSet rs = table_meta.getSchemas();
        try {
          return doFetchResultSet(rs, null, null, null);
        } finally {
          JdbcUtils.closeResultSet(rs);
        }
      }

    });
  }

  // 获取目录
  public JdbcQueryResponse<List<Object>> getCatalogs() throws SQLException {
    return jdbcTemplate.execute(new ConnectionCallback<JdbcQueryResponse<List<Object>>>() {
      @Override
      @Nullable
      public JdbcQueryResponse<List<Object>> doInConnection(Connection conn) throws SQLException, DataAccessException {
        DatabaseMetaData table_meta =  conn.getMetaData();
        ResultSet rs = table_meta.getCatalogs();
        try {
          return doFetchResultSet(rs, null, null, null);
        } finally {
          JdbcUtils.closeResultSet(rs);
        }
      }
    });
  }

  // 获取数据库元信息
  @Override
  public JdbcMetaData getMetaData() throws SQLException {
    if (this.jdbcMetaData != null) return this.jdbcMetaData;
    return this.jdbcMetaData = jdbcTemplate.execute(new ConnectionCallback<JdbcMetaData>() {

      @Override
      @Nullable
      public JdbcMetaData doInConnection(Connection conn) throws SQLException, DataAccessException {
        JdbcMetaData temp_meta = new JdbcMetaData();
        DatabaseMetaData table_meta =  conn.getMetaData();
        temp_meta.setProduct_name(table_meta.getDatabaseProductName());
        temp_meta.setProduct_version(table_meta.getDatabaseProductVersion());
        temp_meta.setDriver_name(table_meta.getDriverName());
        temp_meta.setDriver_version(table_meta.getDriverVersion());
        return temp_meta;
      }

    });
  }

  // 获取表类表
  @Override
  public JdbcQueryResponse<List<Object>> getTables(
    JdbcQueryRequest.ResultMeta return_meta,
    String catalog, String schema_pattern,
    String table_name_pattern, String...types
  ) throws SQLException {
    return jdbcTemplate.execute(new ConnectionCallback<JdbcQueryResponse<List<Object>>>() {

      @Override
      @Nullable
      public JdbcQueryResponse<List<Object>> doInConnection(Connection conn) throws SQLException, DataAccessException {
        DatabaseMetaData table_meta = conn.getMetaData();
        ResultSet rs = table_meta.getTables(catalog, schema_pattern, table_name_pattern, types != null && types.length > 0 ? types : null);
        try {
          return doFetchResultSet(rs, return_meta, null, null);
        } finally {
          JdbcUtils.closeResultSet(rs);
        }
      }

    });
  }
  
  // 测试表是否存在
  public boolean testTableExisted(String table_name, String ...types) {
    long start_time = System.currentTimeMillis();
    try {
      JdbcQueryResponse<List<Object>> table_ret = jdbcTemplate.execute(
        new ConnectionCallback<JdbcQueryResponse<List<Object>>>() {
          @Override
          @Nullable
          public JdbcQueryResponse<List<Object>> doInConnection(Connection conn) throws SQLException, DataAccessException {
            DatabaseMetaData table_meta = conn.getMetaData();
            ResultSet rs = table_meta.getTables(null, null, table_name, types != null && types.length > 0 ? types : null);
            try {
              return doFetchResultSet(rs, null, null, null);
            } finally {
              JdbcUtils.closeResultSet(rs);
            }
          }
        }
      );
      return table_ret != null && table_ret.data.size() > 0;
    } finally {
      log.debug("检查数据表{}是否存在耗时{}毫秒", table_name, (System.currentTimeMillis() - start_time));
    }
  }

  // 获取主键
  @Override
  public Object getTablePrimaryKey(String table_name) throws SQLException {
    return jdbcTemplate.execute(new ConnectionCallback<Object>() {
      @Override
      @Nullable
      public Object doInConnection(Connection conn) throws SQLException, DataAccessException {
        return doFetchTablePrimaryKey(conn, table_name);
      }
    });

  }

  // 执行获取主键
  @SuppressWarnings("unchecked")
  protected Object doFetchTablePrimaryKey(Connection conn, String table_name) throws SQLException {
    Object primary_key = null;
    ResultSet ks = conn.getMetaData().getPrimaryKeys(null, null, table_name);
    try {
      JdbcQueryResponse<List<Object>> list = doFetchResultSet(ks, null, null, null);
      if (list.getData() != null && list.getData().size() > 0) {
        List<Object> primary_key_lst = new LinkedList<>();
        for (Object keycol : list.getData()) {
          Map<String, Object> colobj = (Map<String, Object>)keycol;
          primary_key_lst.add(colobj.get("column_name"));
        }
        if (primary_key_lst.size() > 0) {
          primary_key = primary_key_lst.size() == 1 ? primary_key_lst.get(0) : primary_key_lst;
        }
      }
    } finally {
      JdbcUtils.closeResultSet(ks);
    }
    return primary_key;
  }

  // 执行获取唯一索引
  @SuppressWarnings("unchecked")
  protected List<TableIndex> doFetchTableUniqueIndex(Connection conn, String table_name) throws SQLException {
    List<TableIndex> index_lst = new LinkedList<>();
    ResultSet ks = conn.getMetaData().getIndexInfo(null, null, table_name, true, false);
    try {
      JdbcQueryResponse<List<Object>> list = doFetchResultSet(ks, null, null, null);
      if (list.getData() != null && list.getData().size() > 0) {
        for (Object index_item : list.getData()) {
          Map<String, Object> colobj = (Map<String, Object>)index_item;
          TableIndex index = new TableIndex();
          index.setName((String)colobj.get("index_name"));
          index.setUnique(Objects.equals(colobj.get("non_unique"), Boolean.FALSE));
          index.setOrder((String)colobj.get("asc_or_desc"));
          index.setType((Integer)colobj.get("type"));
          index_lst.add(index);
        }
      }
    } finally {
      JdbcUtils.closeResultSet(ks);
    }
    return index_lst;
  }

  // 执行获取结果集元信息
  protected TableMeta doFetchResultSetMeta(final TableQueryRequest table_query, final SQLDialectProvider sql_dialect_provider) {
    // TODO 实现表元信息缓存
    return jdbcTemplate.execute(new ConnectionCallback<TableMeta>() {
      @Override
      @Nullable
      public TableMeta doInConnection(Connection conn) throws SQLException, DataAccessException {
        if (table_query.hasTable_meta()) {
          return table_query.getTable_meta();
        }
        Object primary_key =  doFetchTablePrimaryKey(conn, table_query.getTable_name());
        List<TableIndex> unique_index = doFetchTableUniqueIndex(conn, table_query.getTable_name());
        PreparedStatement ps = sql_dialect_provider.prepareNoRowSelect(conn, table_query);
        try {
          ResultSet rs = ps.executeQuery();
          try {
            rs = ps.getResultSet();
            return TableMeta.of(rs.getMetaData(), table_query.getResult(), primary_key, unique_index);
          } finally {
            JdbcUtils.closeResultSet(rs);
          }
        } finally {
          JdbcUtils.closeStatement(ps);
        }
      }
    });
  }

  // 元信息
  public TableMeta queryResultMeta(
    TableQueryRequest table_query
  ) throws SQLException {
    if (table_query.hasTable_meta()) return table_query.getTable_meta();
    SQLDialectProvider sql_dialect_provider = getSQLDialectProvider();
    Validate.notNull(sql_dialect_provider, "抱歉，暂时不支持%s!", getMetaData().getProduct_name());
    String table_name = table_query.getTable_name();
    if (!testTableExisted(table_name)) {
      throw new NotFoundException(String.format("%s不存在", table_name));
    }
    return doFetchResultSetMeta(table_query, sql_dialect_provider);
  }

  // 分页查询统计信息
  protected boolean doFetchQueryPageTotal(TableQueryRequest table_query, SQLDialectProvider sql_dialect_provider, JdbcQueryResponse<Page<Object>> pageret) {
    long start_time = System.currentTimeMillis();
    try {
      // 获取统计
      JdbcQuery count_query = sql_dialect_provider.prepareQueryCount(table_query);
      Long query_count = 0l;
      if (count_query.hasParameter()) {
        query_count = jdbcTemplate.queryForObject(count_query.getSql(), Long.class, count_query.getParameters().toArray(new Object[0]));
      } else {
        query_count = jdbcTemplate.queryForObject(count_query.getSql(), Long.class);
      }
      pageret.getData().setTotal(query_count != null ? query_count : 0);
      pageret.getData().setOffset(table_query.getResult().getOffset());

      boolean has_data = true;
      // 获取源信息
      if (pageret.getData().getTotal() == 0 ||
        pageret.getData().getOffset() >= pageret.getData().getTotal() ||
        table_query.getResult().getLimit() <= 0
      ) {
        has_data = false;
        // 要求返回元信息
        if (table_query.getResult().isContain_meta()) {
          if (table_query.hasTable_meta()) {
            pageret.setMeta(table_query.getTable_meta());
          } else {
            TableMeta table_meta = doFetchResultSetMeta(table_query, sql_dialect_provider);
            table_query.setTable_meta(table_meta);
            pageret.setMeta(table_meta);
          }
        }
      }
      return has_data;
    } finally {
      log.debug("执行表{}分页查询统计耗时{}毫秒", table_query.getTable_name(), (System.currentTimeMillis() - start_time));
    }
  }

  // 查询列表结果
  protected JdbcQueryResponse<?> doQueryListResult(TableQueryRequest table_query, SQLDialectProvider sql_dialect_provider) {
    long start_time = System.currentTimeMillis();
    try {
      // 获取列表数据
      JdbcQueryResponse<List<Object>> datalist = jdbcTemplate.execute(
        new ConnectionCallback<JdbcQueryResponse<List<Object>>>() {
          @Override
          @Nullable
          public JdbcQueryResponse<List<Object>> doInConnection(Connection conn) throws SQLException, DataAccessException {
            Object primary_key = null;
            List<TableIndex> unique_index = null;
            if (table_query.hasTable_meta()) {
              primary_key = table_query.getTable_meta().getPrimary_key();
              unique_index = table_query.getTable_meta().getUnique_indexs();
            } else {
              primary_key = doFetchTablePrimaryKey(conn, table_query.getTable_name());
              unique_index = doFetchTableUniqueIndex(conn, table_query.getTable_name());
            }
            PreparedStatement ps = sql_dialect_provider.preparePageSelect(conn, table_query);
            try {
              ResultSet rs = null;
              try {
                rs = ps.executeQuery();
                return doFetchResultSet(rs, table_query.getResult(), primary_key, unique_index);
              } finally {
                if (rs != null) {
                  JdbcUtils.closeResultSet(rs);
                }
              }
            } finally {
              JdbcUtils.closeStatement(ps);
            }
          }
        }
      );
      return datalist;
    } finally {
      log.debug("执行表{}数据行查询耗时{}毫秒", table_query.getTable_name(), (System.currentTimeMillis() - start_time));
    }
  }

  @Override
  @SuppressWarnings({"rawtypes", "unchecked"})
  public JdbcQueryResponse<?> queryPageResult(
    TableQueryRequest table_query
  ) throws SQLException {
    SQLDialectProvider sql_dialect_provider = getSQLDialectProvider();
    Validate.notNull(sql_dialect_provider, "抱歉，暂时不支持%s!", getMetaData().getProduct_name());

    long start_time = System.currentTimeMillis();
    String table_name = table_query.getTable_name();

    if (!table_query.hasTable_meta()) { // 无表元信息则查询
      if (!testTableExisted(table_name)) {
        throw new NotFoundException(String.format("数据表%s不存在", table_name));
      }
    }

    final JdbcQueryResponse<Page<Object>> pageret = new JdbcQueryResponse<Page<Object>>();
    pageret.setData(new Page<Object>());

    // 获取分页统计
    if (table_query.getResult().isRow_total() && !this.doFetchQueryPageTotal(table_query, sql_dialect_provider, pageret)) {
      return pageret;
    }

    // 获取分页数据
    JdbcQueryResponse<?> datalist = this.doQueryListResult(table_query, sql_dialect_provider);

    // 返回数据
    if (datalist != null && datalist.getData() instanceof List) {
      pageret.setMeta(datalist.getMeta());
      pageret.getData().setOffset(table_query.getResult().getOffset());
      pageret.getData().setData((List)datalist.getData());
    }
    log.debug("执行表{}分页查询总耗时{}毫秒", table_query.getTable_name(), (System.currentTimeMillis() - start_time));
    return pageret;
  }

  protected SQLDialectProvider getSQLDialectProvider() throws SQLException {
    JdbcMetaData metadata = getMetaData();
    return this.sqlDialectProviders.get(metadata.getProduct_name().toLowerCase());
  }

  @Override
  @SuppressWarnings("unchecked")
  public long queryAllCountResult(TableQueryRequest table_query) throws SQLException {
    SQLDialectProvider sql_dialect_provider = getSQLDialectProvider();
    Validate.notNull(sql_dialect_provider, "抱歉，暂时不支持%s!", getMetaData().getProduct_name());
    table_query.getResult().setRow_struct(JdbcQueryRequest.ResultMeta.ResultObjectStruct.list);
    table_query.getResult().setLimit(1l);
    JdbcQueryResponse<?> datalist = this.doQueryListResult(table_query, sql_dialect_provider);
    return (long)((List<Object>)(((List<Object>)datalist.getData()).get(0))).get(0);
  }

  @Override
  public JdbcQueryResponse<?> queryGroupCountResult(TableQueryRequest table_query) throws SQLException {
    SQLDialectProvider sql_dialect_provider = getSQLDialectProvider();
    Validate.notNull(sql_dialect_provider, "抱歉，暂时不支持%s!", getMetaData().getProduct_name());
    table_query.getResult().setLimit(Constants.DEFAULT_MAX_LIMIT);
    JdbcQueryResponse<?> datalist = this.doQueryListResult(table_query, sql_dialect_provider);
    return datalist;
  }

  @Override
  public JdbcQueryResponse<?> queryGroupResult(TableQueryRequest table_query) throws SQLException {
    SQLDialectProvider sql_dialect_provider = getSQLDialectProvider();
    Validate.notNull(sql_dialect_provider, "抱歉，暂时不支持%s!", getMetaData().getProduct_name());
    table_query.getResult().setLimit(Constants.DEFAULT_MAX_LIMIT);
    JdbcQueryResponse<?> datalist = this.doQueryListResult(table_query, sql_dialect_provider);
    return datalist;
  }

  @Override
  @SuppressWarnings("unchecked")
  public JdbcQueryResponse<?> querySignletonResult(TableQueryRequest table_query) throws SQLException {
    SQLDialectProvider sql_dialect_provider = getSQLDialectProvider();
    Validate.notNull(sql_dialect_provider, "抱歉，暂时不支持%s!", getMetaData().getProduct_name());
    table_query.getResult().setLimit(1l);
    JdbcQueryResponse<?> datalist = this.doQueryListResult(table_query, sql_dialect_provider);
    if (((List<Object>)datalist.getData()).size() == 0) {
      JdbcQueryResponse<Object> response = new JdbcQueryResponse<>();
      return response;
    }
    Object data = ((List<Object>)datalist.getData()).get(0);
    JdbcQueryResponse<Object> ret = new JdbcQueryResponse<>();
    ret.setData(data);
    ret.setMeta(datalist.getMeta());
    return ret;
  }

  protected Object doInsertTableData(
    ManipulationRequest insert_request,
    SQLDialectProvider sql_dialect_provider
  ) throws SQLException {
    return jdbcTemplate.execute(new ConnectionCallback<Object>() {
      @Override
      @Nullable
      public Object doInConnection(Connection conn) throws SQLException, DataAccessException {
        PreparedStatement ps = sql_dialect_provider.prepareDataInsert(conn, insert_request);
        try {
          List<Map<String, Object>> input_datas = insert_request.getInput_list();
          for (Map<String, Object> input_data : input_datas) {
            int i = 1;
            for (TableColumn v : insert_request.getColumns()) {
              if (v.getRead_only() != null && v.getRead_only()) continue;
              if (v.getAuto_increment() != null && v.getAuto_increment()) continue;
              if (v.getWritable() != null && v.getWritable()) {
                Object val = JdbcUtils.convert(input_data.get(v.getName()), v.getJdbc_type());
                ps.setObject(i, val);
                i = i + 1;
              }
            }
            ps.addBatch();
          }
          int inserted[] = ps.executeBatch();
          if (!insert_request.hasAutoGenerated()) {
            return inserted;
          }
          ResultSet auto_keys = ps.getGeneratedKeys();
          List<Integer> key_list = new LinkedList<>();
          while(auto_keys.next()) {
            key_list.add(auto_keys.getInt(1));
          }
          return new Object[] {
            inserted, key_list.toArray(new Integer[0])
          };
        } finally {
          JdbcUtils.closeStatement(ps);
        }
      }
    });
  }

  // 执行更新
  protected int[] doUpdateTableData(
    ManipulationRequest update_request,
    SQLDialectProvider sql_dialect_provider
  ) throws SQLException {
    return jdbcTemplate.execute(new ConnectionCallback<int[]>() {
      @Override
      @Nullable
      public int[] doInConnection(Connection conn) throws SQLException, DataAccessException {
        PreparedStatement ps = sql_dialect_provider.prepareDataUpdate(conn, update_request);
        try {
          List<Map<String, Object>> input_datas = update_request.getInput_list();
          for (Map<String, Object> input_data : input_datas) {
            int i = 1;
            for (TableColumn v : update_request.getColumns()) {
              if (v.getRead_only() != null && v.getRead_only()) continue;
              if (v.getAuto_increment() != null && v.getAuto_increment()) continue;
              if (update_request.testRow_key(v.getName())) continue;
              if (v.getWritable() != null && v.getWritable()) {
                Object val = JdbcUtils.convert(input_data.get(v.getName()), v.getJdbc_type());
                log.info("{} = {}", v.getName(), val);
                ps.setObject(i, val);
                i = i + 1;
              }
            }
            Object keyvalue = update_request.getInput_key();
            Object rowkey = update_request.getRow_key();
            if (update_request.testComposite_key()) {
              List<?> row_keys = (List<?>)rowkey;
              List<?> key_values = (List<?>)keyvalue;
              for (int j = 0; j < row_keys.size(); j++) {
                String keyname = (String)row_keys.get(j);
                Object keyval = key_values.get(j);
                TableColumn keycol = update_request.findColumn(keyname);
                ps.setObject(i, JdbcUtils.convert(
                  keyval, keycol != null ? keycol.getJdbc_type() : null
                ));
                i = i + 1;
              }
            } else {
              TableColumn keycol = update_request.findColumn((String)rowkey);
              ps.setObject(i, JdbcUtils.convert(
                keyvalue, keycol != null ? keycol.getJdbc_type() : null
              ));
              i = i + 1;
            }
            ps.addBatch();
          }
          return ps.executeBatch();
        } finally {
          JdbcUtils.closeStatement(ps);
        }
      }
    });
  }

  // 插入表数据
  @Override
  public Object insertTableData(
    ManipulationRequest insert_request
  ) throws SQLException {
    SQLDialectProvider sql_dialect_provider = getSQLDialectProvider();
    Validate.notNull(sql_dialect_provider, "抱歉，暂时不支持%s!", getMetaData().getProduct_name());
    Object result = doInsertTableData(insert_request, sql_dialect_provider);
    if (insert_request.testSignletonData()) {
      if (insert_request.hasAutoGenerated()) {
        return new Object[] {
          Array.get(Array.get(result, 0), 0),
          Array.get(Array.get(result, 1), 0),
        };
      } else {
        return Array.get(result, 0);
      }
    } else {
      return result;
    }
  }

  // 更新表数据
  public Object updateTableData(
    ManipulationRequest update_request
  ) throws SQLException {
    SQLDialectProvider sql_dialect_provider = getSQLDialectProvider();
    Validate.notNull(sql_dialect_provider, "抱歉，暂时不支持%s!", getMetaData().getProduct_name());
    int []result = doUpdateTableData(update_request, sql_dialect_provider);
    if (update_request.testSignletonData()) {
      return Array.get(result, 0);
    } else {
      return result;
    }
  }

}
