package com.snz1.jdbc.rest.service.impl;

import java.io.ByteArrayInputStream;
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

import com.snz1.jdbc.rest.data.JdbcMetaData;
import com.snz1.jdbc.rest.data.JdbcQuery;
import com.snz1.jdbc.rest.data.JdbcQueryRequest;
import com.snz1.jdbc.rest.data.JdbcQueryResponse;
import com.snz1.jdbc.rest.data.JdbcQueryResponse.ResultMeta;
import com.snz1.jdbc.rest.data.JdbcQueryResponse.ResultMeta.ResultColumn;
import com.snz1.jdbc.rest.data.TableQueryRequest;
import com.snz1.jdbc.rest.service.JdbcRestProvider;
import com.snz1.jdbc.rest.service.SQLDialectProvider;
import com.snz1.jdbc.rest.utils.JdbcUtils;
import com.snz1.utils.JsonUtils;

import gateway.api.NotFoundException;
import gateway.api.Page;
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

  @SuppressWarnings("null")
  protected JdbcQueryResponse<List<Object>> fetchResultSet(ResultSet rs, JdbcQueryRequest.ResultMeta return_meta, Object primary_key) throws SQLException {
    boolean onepack = true; 
    boolean meta = false;
    boolean objlist = false;

    if (return_meta != null) {
      meta = return_meta.isContain_meta();
      onepack = return_meta.isColumn_compact();
      objlist = JdbcQueryRequest.ResultMeta.ResultObjectStruct.list.equals(return_meta.getRow_struct());
    }

    ResultMeta result_meta = ResultMeta.of(rs.getMetaData(), return_meta, primary_key);
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
      for (ResultColumn col_item : result_meta.getColumns()) {
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

  public JdbcQueryResponse<List<Object>> getSchemas() throws SQLException {
    return jdbcTemplate.execute(new ConnectionCallback<JdbcQueryResponse<List<Object>>>() {

      @Override
      @Nullable
      public JdbcQueryResponse<List<Object>> doInConnection(Connection conn) throws SQLException, DataAccessException {
        DatabaseMetaData table_meta =  conn.getMetaData();
        ResultSet rs = table_meta.getSchemas();
        try {
          return fetchResultSet(rs, null, null);
        } finally {
          rs.close();
        }
      }

    });
  }

  public JdbcQueryResponse<List<Object>> getCatalogs() throws SQLException {
    return jdbcTemplate.execute(new ConnectionCallback<JdbcQueryResponse<List<Object>>>() {
      @Override
      @Nullable
      public JdbcQueryResponse<List<Object>> doInConnection(Connection conn) throws SQLException, DataAccessException {
        DatabaseMetaData table_meta =  conn.getMetaData();
        ResultSet rs = table_meta.getCatalogs();
        try {
          return fetchResultSet(rs, null, null);
        } finally {
          rs.close();
        }
      }
    });
  }

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
          return fetchResultSet(rs, return_meta, null);
        } finally {
          rs.close();
        }
      }

    });
  }
  

  private boolean testTableExisted(String table_name, String ...types) {
    JdbcQueryResponse<List<Object>> table_ret = jdbcTemplate.execute(new ConnectionCallback<JdbcQueryResponse<List<Object>>>() {

      @Override
      @Nullable
      public JdbcQueryResponse<List<Object>> doInConnection(Connection conn) throws SQLException, DataAccessException {
        DatabaseMetaData table_meta = conn.getMetaData();
        ResultSet rs = table_meta.getTables(null, null, table_name, types != null && types.length > 0 ? types : null);
        try {
          return fetchResultSet(rs, null, null);
        } finally {
          rs.close();
        }
      }

    });
    return table_ret != null && table_ret.data.size() > 0;
  }

  @SuppressWarnings("unchecked")
  protected Object fetchTablePrimaryKey(Connection conn, String table_name) throws SQLException {
    Object primary_key = null;
    ResultSet ks = conn.getMetaData().getPrimaryKeys(null, null, table_name);
    try {
      JdbcQueryResponse<List<Object>> list = fetchResultSet(ks, null, null);
      if (log.isDebugEnabled()) {
        log.info("primary keys: " + JsonUtils.toJson(list.getData()));
      }
      if (list.getData() != null && list.getData().size() > 0) {
        List<Object> primary_key_lst = new LinkedList<>();
        for (Object keycol : list.getData()) {
          Map<String, Object> colobj = (Map<String, Object>)keycol;
          primary_key_lst.add(colobj.get("column_name"));
        }
        primary_key = primary_key_lst.size() == 1 ? primary_key_lst.get(0) : primary_key_lst;
      }
    } finally {
      ks.close();
    }
    return primary_key;
  }

  protected ResultMeta fetchResultSetMeta(final TableQueryRequest table_query, final SQLDialectProvider sql_dialect_provider) {
    return jdbcTemplate.execute(new ConnectionCallback<ResultMeta>() {
      @Override
      @Nullable
      public ResultMeta doInConnection(Connection conn) throws SQLException, DataAccessException {
        Object primary_key = fetchTablePrimaryKey(conn, table_query.getTable_name());
        PreparedStatement ps = sql_dialect_provider.prepareNoRowSelect(conn, table_query);
        try {
          ResultSet rs = ps.executeQuery();
          try {
            rs = ps.getResultSet();
            return ResultMeta.of(rs.getMetaData(), table_query.getResult(), primary_key);
          } finally {
            rs.close();
          }
        } finally {
          ps.close();
        }
      }
    });
  }

  // 元信息
  public JdbcQueryResponse.ResultMeta queryResultMeta(
    TableQueryRequest table_query
  ) throws SQLException {
    SQLDialectProvider sql_dialect_provider = getSQLDialectProvider();
    Validate.notNull(sql_dialect_provider, "抱歉，暂时不支持%s!", getMetaData().getProduct_name());

    String table_name = table_query.getTable_name();
    if (!testTableExisted(table_name)) {
      throw new NotFoundException(String.format("%s不存在", table_name));
    }
    return fetchResultSetMeta(table_query, sql_dialect_provider);
  }

  @Override
  public JdbcQueryResponse<Page<Object>> queryPageResult(
    TableQueryRequest table_query
  ) throws SQLException {
    SQLDialectProvider sql_dialect_provider = getSQLDialectProvider();
    Validate.notNull(sql_dialect_provider, "抱歉，暂时不支持%s!", getMetaData().getProduct_name());

    String table_name = table_query.getTable_name();
    if (!testTableExisted(table_name)) {
      throw new NotFoundException(String.format("%s不存在", table_name));
    }

    final JdbcQueryResponse<Page<Object>> pageret = new JdbcQueryResponse<Page<Object>>();
    pageret.setData(new Page<Object>());

    // 获取统计
    JdbcQuery count_query = sql_dialect_provider.prepareQueryCount(table_query);
    Long query_count = 0l;
    if (count_query.hasParameter()) {
      query_count = jdbcTemplate.queryForObject(count_query.getSql(), Long.class, count_query.getParameters().toArray(new Object[0]));
    } else {
      query_count = jdbcTemplate.queryForObject(count_query.getSql(), Long.class);
    }
    pageret.getData().total = query_count != null ? query_count : 0;
    pageret.getData().offset = table_query.getResult().getOffset();

    // 获取源信息
    if (pageret.getData().total == 0 ||
      pageret.getData().offset >= pageret.getData().total ||
      table_query.getResult().getLimit() <= 0) {
      // 要求返回元信息
      if (table_query.getResult().isContain_meta()) {
        pageret.setMeta(fetchResultSetMeta(table_query, sql_dialect_provider));
      }
    }

    // 获取数据
    JdbcQueryResponse<List<Object>> datalist = jdbcTemplate.execute(new ConnectionCallback<JdbcQueryResponse<List<Object>>>() {
      @Override
      @Nullable
      public JdbcQueryResponse<List<Object>> doInConnection(Connection conn) throws SQLException, DataAccessException {
        Object primary_key = fetchTablePrimaryKey(conn, table_query.getTable_name());
        PreparedStatement ps = sql_dialect_provider.preparePageSelect(conn, table_query);
        try {
          ResultSet rs = null;
          try {
            rs = ps.executeQuery();
            return fetchResultSet(rs, table_query.getResult(), primary_key);
          } finally {
            if (rs != null) {
              rs.close();
            }
          }
        } finally {
          ps.close();
        }
      }
    });

    // 返回数据
    if (datalist != null) {
      pageret.setMeta(datalist.getMeta());
      pageret.getData().data = datalist.getData();
    }

    return pageret;
  }

  protected SQLDialectProvider getSQLDialectProvider() throws SQLException {
    JdbcMetaData metadata = getMetaData();
    return this.sqlDialectProviders.get(metadata.getProduct_name().toLowerCase());
  }

  @Override
  public int queryAllCountResult(TableQueryRequest table_query) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public JdbcQueryResponse<Page<Object>> queryGroupCountResult(TableQueryRequest table_query) {
    // TODO Auto-generated method stub
    return null;
  }

}
