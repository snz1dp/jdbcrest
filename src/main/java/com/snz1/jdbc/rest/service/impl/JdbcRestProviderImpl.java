package com.snz1.jdbc.rest.service.impl;

import java.io.ByteArrayInputStream;
import java.lang.reflect.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Resource;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.Validate;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;

import com.snz1.jdbc.rest.Constants;
import com.snz1.jdbc.rest.data.ManipulationRequest;
import com.snz1.jdbc.rest.data.JdbcDMLRequest;
import com.snz1.jdbc.rest.data.JdbcDMLResponse;
import com.snz1.jdbc.rest.data.JdbcMetaData;
import com.snz1.jdbc.rest.data.JdbcQueryStatement;
import com.snz1.jdbc.rest.data.JdbcQueryResponse;
import com.snz1.jdbc.rest.data.Page;
import com.snz1.jdbc.rest.data.ResultDefinition;
import com.snz1.jdbc.rest.data.SQLServiceDefinition;
import com.snz1.jdbc.rest.data.SQLServiceRequest;
import com.snz1.jdbc.rest.data.TableMeta;
import com.snz1.jdbc.rest.data.TableColumn;
import com.snz1.jdbc.rest.data.TableDefinition;
import com.snz1.jdbc.rest.data.TableIndex;
import com.snz1.jdbc.rest.data.JdbcQueryRequest;
import com.snz1.jdbc.rest.data.WhereCloumn;
import com.snz1.jdbc.rest.service.JdbcRestProvider;
import com.snz1.jdbc.rest.service.JdbcTypeConverterFactory;
import com.snz1.jdbc.rest.service.LoggedUserContext;
import com.snz1.jdbc.rest.service.SQLDialectProvider;
import com.snz1.jdbc.rest.service.TableDefinitionRegistry;
import com.snz1.jdbc.rest.service.LoggedUserContext.UserInfo;
import com.snz1.jdbc.rest.utils.JdbcUtils;
import com.snz1.utils.JsonUtils;
import com.snz1.utils.WebUtils;

import org.apache.ibatis.mapping.SqlCommandType;

import gateway.api.NotFoundException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class JdbcRestProviderImpl implements JdbcRestProvider {

  private JdbcMetaData jdbcMetaData;

  @Resource
  private JdbcTemplate jdbcTemplate;

  @Resource
  private SqlSessionFactory sessionFactory;

  @Resource
  private TableDefinitionRegistry tableDefinitionRegistry;

  @Resource
  private LoggedUserContext loggedUserContext;

  @Resource
  private JdbcTypeConverterFactory typeConverterFactory;

  private Map<String, SQLDialectProvider> sqlDialectProviders;

  public JdbcTypeConverterFactory getTypeConverterFactory() {
    return this.typeConverterFactory;
  }

  @EventListener(ContextRefreshedEvent.class)
  public void loadSQLDialectProviders(ContextRefreshedEvent event) {
    final Map<String, SQLDialectProvider> map = new LinkedHashMap<>();
    event.getApplicationContext().getBeansOfType(SQLDialectProvider.class).forEach((k, v) -> {
      map.put(v.getName(), v);
    });
    this.sqlDialectProviders = new HashMap<>(map);
  }

  // 执行获取结果集
  @SuppressWarnings("null")
  protected JdbcQueryResponse<List<Object>> doFetchResultSet(
    ResultSet rs, ResultDefinition return_meta,
    Object primary_key, List<TableIndex> unique_index,
    TableDefinition table_definition
  ) throws SQLException {
    boolean onepack = true; 
    boolean meta = false;
    boolean objlist = false;

    if (return_meta != null) {
      meta = return_meta.isContain_meta();
      onepack = return_meta.isColumn_compact();
      objlist = ResultDefinition.ResultRowStruct.list.equals(return_meta.getRow_struct());
    }

    TableMeta result_meta = TableMeta.of(
      rs.getMetaData(),
      return_meta,
      primary_key,
      unique_index,
      table_definition
    );
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
          ResultDefinition.ResultColumn coldef = return_meta != null ? return_meta.getColumns().get(col_item.getName()) : null;
          if (coldef != null && !Objects.equals(coldef.getType(), ResultDefinition.ResultType.raw)) {
            if (Objects.equals(coldef.getType(), ResultDefinition.ResultType.list)) {
              if (col_item.getJdbc_type() == JDBCType.BLOB) {
                col_obj = JsonUtils.fromJson(new ByteArrayInputStream((byte[])col_obj), List.class);
              } else {
                col_obj = JsonUtils.fromJson(col_obj.toString(), List.class);
              }
            } else if (Objects.equals(coldef.getType(), ResultDefinition.ResultType.map)) {
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
          return doFetchResultSet(rs, null, null, null, null);
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
          return doFetchResultSet(rs, null, null, null, null);
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
    ResultDefinition return_meta,
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
          return doFetchResultSet(rs, return_meta, null, null, null);
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
              return doFetchResultSet(rs, null, null, null, null);
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
      JdbcQueryResponse<List<Object>> list = doFetchResultSet(ks, null, null, null, null);
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
      JdbcQueryResponse<List<Object>> list = doFetchResultSet(ks, null, null, null, null);
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
  protected TableMeta doFetchResultSetMeta(final JdbcQueryRequest table_query, final SQLDialectProvider sql_dialect_provider) {
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
            return TableMeta.of(
              rs.getMetaData(),
              table_query.getResult(),
              primary_key,
              unique_index,
              table_query.getDefinition()
            );
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
    JdbcQueryRequest table_query
  ) throws SQLException {
    if (table_query.hasTable_meta()) return table_query.getTable_meta();
    SQLDialectProvider sql_dialect_provider = getSQLDialectProvider();
    Validate.notNull(sql_dialect_provider, "抱歉，暂时不支持%s!", getMetaData().getProduct_name());
    String table_name = table_query.getTable_name();
    if (tableDefinitionRegistry.hasTableDefinition(table_name)) {
      TableDefinition definition = tableDefinitionRegistry.getTableDefinition(table_name);
      table_query.setDefinition(definition);
      if (definition.hasAlias_name()) {
        table_name = definition.getAlias();
      }
    }
    if (!testTableExisted(table_name)) {
      throw new NotFoundException(String.format("%s不存在", table_name));
    }
    table_query.setTable_name(table_name);
    return doFetchResultSetMeta(table_query, sql_dialect_provider);
  }

  // 分页查询统计信息
  protected boolean doFetchQueryPageTotal(JdbcQueryRequest table_query, SQLDialectProvider sql_dialect_provider, JdbcQueryResponse<Page<Object>> pageret) {
    long start_time = System.currentTimeMillis();
    try {
      // 获取统计
      JdbcQueryStatement count_query = sql_dialect_provider.prepareQueryCount(table_query);
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
  protected JdbcQueryResponse<?> doQueryListResult(JdbcQueryRequest table_query, SQLDialectProvider sql_dialect_provider) {
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
                return doFetchResultSet(
                  rs, table_query.getResult(),
                  primary_key, unique_index,
                  table_query.getDefinition()
                );
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
    JdbcQueryRequest table_query
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
  public long queryAllCountResult(JdbcQueryRequest table_query) throws SQLException {
    SQLDialectProvider sql_dialect_provider = getSQLDialectProvider();
    Validate.notNull(sql_dialect_provider, "抱歉，暂时不支持%s!", getMetaData().getProduct_name());
    table_query.getResult().setRow_struct(ResultDefinition.ResultRowStruct.list);
    table_query.getResult().setLimit(1l);
    JdbcQueryResponse<?> datalist = this.doQueryListResult(table_query, sql_dialect_provider);
    return (long)((List<Object>)(((List<Object>)datalist.getData()).get(0))).get(0);
  }

  @Override
  public JdbcQueryResponse<?> queryGroupCountResult(JdbcQueryRequest table_query) throws SQLException {
    SQLDialectProvider sql_dialect_provider = getSQLDialectProvider();
    Validate.notNull(sql_dialect_provider, "抱歉，暂时不支持%s!", getMetaData().getProduct_name());
    table_query.getResult().setLimit(Constants.DEFAULT_MAX_LIMIT);
    JdbcQueryResponse<?> datalist = this.doQueryListResult(table_query, sql_dialect_provider);
    return datalist;
  }

  @Override
  public JdbcQueryResponse<?> queryGroupResult(JdbcQueryRequest table_query) throws SQLException {
    SQLDialectProvider sql_dialect_provider = getSQLDialectProvider();
    Validate.notNull(sql_dialect_provider, "抱歉，暂时不支持%s!", getMetaData().getProduct_name());
    table_query.getResult().setLimit(Constants.DEFAULT_MAX_LIMIT);
    JdbcQueryResponse<?> datalist = this.doQueryListResult(table_query, sql_dialect_provider);
    return datalist;
  }

  @Override
  @SuppressWarnings("unchecked")
  public JdbcQueryResponse<?> querySignletonResult(JdbcQueryRequest table_query) throws SQLException {
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

  // 构建
  private Map<String, Object> doBuildInsertRequestInputData(ManipulationRequest insert_request, Map<String, Object> input_data) {
    TableDefinition definition = insert_request.getDefinition();
    if (definition == null) {
      return input_data;
    }
    input_data = new HashMap<>(input_data);
    if (definition.hasCreated_time_column()) {
      input_data.put(definition.getCreated_time_column(), insert_request.getRequest_time());
    }
    if (definition.hasUpdated_time_column()) {
      input_data.put(definition.getUpdated_time_column(), insert_request.getRequest_time());
    }

    UserInfo logged_user = null;
    if (loggedUserContext.isUserLogged()) {
      logged_user = loggedUserContext.getLoginUserInfo();
    }

    if (definition.hasCreator_id_column()) {
      TableDefinition.UserIdColumn userid = definition.getCreator_id_column();
      if (logged_user != null) {
        input_data.put(userid.getName(), logged_user.getIdByType(userid.getIdtype()));
      } else {
        input_data.put(userid.getName(), null);
      }
    }

    if (definition.hasCreator_name_column()) {
      if (logged_user != null) {
        input_data.put(definition.getCreator_name_column(), logged_user.getDisplay_name());
      } else {
        input_data.put(definition.getCreator_name_column(), null);
      }
    }

    if (definition.hasMender_id_column()) {
      TableDefinition.UserIdColumn userid = definition.getMender_id_column();
      if (logged_user != null) {
        input_data.put(userid.getName(), logged_user.getIdByType(userid.getIdtype()));
      } else {
        input_data.put(userid.getName(), null);
      }
    }

    if (definition.hasMender_name_column()) {
      if (logged_user != null) {
        input_data.put(definition.getMender_name_column(), logged_user.getDisplay_name());
      } else {
        input_data.put(definition.getMender_name_column(), null);
      }
    }
    return input_data;
  }

  // 构建
  private Map<String, Object> doBuildUpdateRequestInputData(ManipulationRequest update_request, Map<String, Object> input_data) {
    TableDefinition definition = update_request.getDefinition();
    if (definition == null) return input_data;
    input_data = new LinkedHashMap<>(input_data);

    if (definition.hasUpdated_time_column()) {
      input_data.put(definition.getUpdated_time_column(), update_request.getRequest_time());
    }

    UserInfo logged_user = null;
    if (loggedUserContext.isUserLogged()) {
      logged_user = loggedUserContext.getLoginUserInfo();
    }

    if (definition.hasUpdated_time_column()) {
      input_data.put(definition.getUpdated_time_column(), update_request.getRequest_time());
    }

    if (definition.hasMender_id_column()) {
      TableDefinition.UserIdColumn userid = definition.getMender_id_column();
      if (logged_user != null) {
        input_data.put(userid.getName(), logged_user.getIdByType(userid.getIdtype()));
      } else {
        input_data.put(userid.getName(), null);
      }
    }

    if (definition.hasMender_name_column()) {
      if (logged_user != null) {
        input_data.put(definition.getMender_name_column(), logged_user.getDisplay_name());
      } else {
        input_data.put(definition.getMender_name_column(), null);
      }
    }

    if (definition.hasOwner_id_column()) {
      TableDefinition.UserIdColumn userid = definition.getOwner_id_column();
      if (logged_user != null) {
        input_data.put(userid.getName(), logged_user.getIdByType(userid.getIdtype()));
      } else {
        input_data.put(userid.getName(), null);
      }
    }

    return input_data;
  }

  protected Object doInsertTableData(
    ManipulationRequest insert_request,
    SQLDialectProvider sql_dialect_provider,
    JdbcTypeConverterFactory type_converter_factory
  ) throws SQLException {
    return jdbcTemplate.execute(new ConnectionCallback<Object>() {
      @Override
      @Nullable
      public Object doInConnection(Connection conn) throws SQLException, DataAccessException {
        PreparedStatement ps = sql_dialect_provider.prepareDataInsert(conn, insert_request);
        try {
          List<Map<String, Object>> input_datas = insert_request.getInput_list();
          for (Map<String, Object> input_data : input_datas) {
            input_data = doBuildInsertRequestInputData(insert_request, input_data);
            int i = 1;
            for (TableColumn v : insert_request.getColumns()) {
              if (v.getRead_only() != null && v.getRead_only()) continue;
              if (v.getAuto_increment() != null && v.getAuto_increment()) continue;
              if (v.getWritable() != null && v.getWritable()) {
                Object val = type_converter_factory.convertObject(input_data.get(v.getName()), v.getJdbc_type());
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
    SQLDialectProvider sql_dialect_provider,
    JdbcTypeConverterFactory type_converter_factory
  ) throws SQLException {
    return jdbcTemplate.execute(new ConnectionCallback<int[]>() {
      @Override
      @Nullable
      public int[] doInConnection(Connection conn) throws SQLException, DataAccessException {
        TableDefinition tdf = update_request.getDefinition();
        PreparedStatement ps = sql_dialect_provider.prepareDataUpdate(conn, update_request);
        try {
          List<Map<String, Object>> input_datas = update_request.getInput_list();
          for (Map<String, Object> input_data : input_datas) {
            int i = 1;
            input_data = doBuildUpdateRequestInputData(update_request, input_data);
            if (update_request.hasWhere() || update_request.isPatch_update()) {
              for (TableColumn v : update_request.getColumns()) {
                if (!input_data.containsKey(v.getName())) continue;
                if (v.getRead_only() != null && v.getRead_only()) continue;
                if (v.getAuto_increment() != null && v.getAuto_increment()) continue;
                if (tdf != null && tdf.inColumn(v.getName())) continue;
                if (v.getWritable() != null && v.getWritable()) {
                  if (log.isDebugEnabled()) {
                    log.debug("设置更新参数: PI={}, COLUMN={}, VALUE={}", i, v.getName(), input_data.get(v.getName()));
                  }
                  ps.setObject(i, type_converter_factory.convertObject(input_data.get(v.getName()), v.getJdbc_type()));
                  i = i + 1;
                }
              }
            } else {
              for (TableColumn v : update_request.getColumns()) {
                if (v.getRead_only() != null && v.getRead_only()) continue;
                if (v.getAuto_increment() != null && v.getAuto_increment()) continue;
                if (update_request.testRow_key(v.getName())) continue;
                if (tdf != null && tdf.inColumn(v.getName())) continue;
                if (v.getWritable() != null && v.getWritable()) {
                  if (log.isDebugEnabled()) {
                    log.debug("设置更新参数: PI={}, COLUMN={}, VALUE={}", i, v.getName(), input_data.get(v.getName()));
                  }
                  ps.setObject(i, type_converter_factory.convertObject(input_data.get(v.getName()), v.getJdbc_type()));
                  i = i + 1;
                }
              }
            }

            if (tdf != null) {
              if (tdf.hasUpdated_time_column()) {
                TableColumn v = update_request.findColumn(tdf.getUpdated_time_column());
                if (log.isDebugEnabled()) {
                  log.debug("设置上下文更新参数: PI={}, COLUMN={}, VALUE={}", i, v.getName(), input_data.get(v.getName()));
                }
                ps.setObject(i, type_converter_factory.convertObject(input_data.get(v.getName()), v.getJdbc_type()));
                i = i + 1;
              }
      
              if (tdf.hasMender_id_column()) {
                TableColumn v = update_request.findColumn(tdf.getMender_id_column().getName());
                if (log.isDebugEnabled()) {
                  log.debug("设置上下文更新参数: PI={}, COLUMN={}, VALUE={}", i, v.getName(), input_data.get(v.getName()));
                }
                ps.setObject(i, type_converter_factory.convertObject(input_data.get(v.getName()), v.getJdbc_type()));
                i = i + 1;
              }
      
              if (tdf.hasMender_name_column()) {
                TableColumn v = update_request.findColumn(tdf.getMender_name_column());
                if (log.isDebugEnabled()) {
                  log.debug("设置上下文更新参数: PI={}, COLUMN={}, VALUE={}", i, v.getName(), input_data.get(v.getName()));
                }
                ps.setObject(i, type_converter_factory.convertObject(input_data.get(v.getName()), v.getJdbc_type()));
                i = i + 1;
              }
            }

            if (update_request.hasWhere()) {
              List<Object> where_conditions = new LinkedList<>();
              for (WhereCloumn w : update_request.getWhere()) {
                w.buildParameters(where_conditions, type_converter_factory);
              }
              for (Object where_object : where_conditions) {
                ps.setObject(i, where_object);
                if (log.isDebugEnabled()) {
                  log.debug("设置更新条件参数: PI={}, WHERE={}", i, where_object);
                }
                i = i + 1;
              }
            } else {
              Object keyvalue = update_request.getInput_key();
              Object rowkey = update_request.getRow_key();
              if (update_request.testComposite_key()) {
                List<?> row_keys = (List<?>)rowkey;
                List<?> key_values = (List<?>)keyvalue;
                for (int j = 0; j < row_keys.size(); j++) {
                  String keyname = (String)row_keys.get(j);
                  Object keyval = key_values.get(j);
                  TableColumn v = update_request.findColumn(keyname);
                  if (log.isDebugEnabled()) {
                    log.debug("设置多主键条件参数: PI={}, COLUMN={}, VALUE={}", i, v.getName(), keyval);
                  }
                  ps.setObject(i, type_converter_factory.convertObject(
                    keyval, v != null ? v.getJdbc_type() : null
                  ));
                  i = i + 1;
                }
              } else {
                TableColumn v = update_request.findColumn((String)rowkey);
                if (log.isDebugEnabled()) {
                  log.debug("设置单主键条件参数: PI={}, COLUMN={}, VALUE={}", i, v.getName(), keyvalue);
                }
                ps.setObject(i, type_converter_factory.convertObject(
                  keyvalue, v != null ? v.getJdbc_type() : null
                ));
                i = i + 1;
              }
            }

            if (tdf != null && tdf.hasOwner_id_column()) {
              TableColumn v = update_request.findColumn(tdf.getOwner_id_column().getName());
              if (log.isDebugEnabled()) {
                log.debug("设置上下文条件参数: PI={}, COLUMN={}, VALUE={}", i, v.getName(), input_data.get(v.getName()));
              }
              ps.setObject(i, type_converter_factory.convertObject(input_data.get(v.getName()), v.getJdbc_type()));
              i = i + 1;
            }

            if (tdf != null && tdf.hasDefault_where()) {
              List<Object> where_conditions = new LinkedList<>();
              List<WhereCloumn> copied_where = tdf.copyDefault_where();
              for (WhereCloumn w : copied_where) {
                w.buildParameters(where_conditions, type_converter_factory);
              }
              for (Object where_object : where_conditions) {
                ps.setObject(i, where_object);
                if (log.isDebugEnabled()) {
                  log.debug("设置缺省定义条件参数: PI={}, WHERE={}", i, where_object);
                }
                i = i + 1;
              }
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

  // 执行更新
  protected Integer doDeleteTableData(
    ManipulationRequest update_request,
    SQLDialectProvider sql_dialect_provider,
    JdbcTypeConverterFactory type_converter_factory
  ) throws SQLException {
    return jdbcTemplate.execute(new ConnectionCallback<Integer>() {
      @Override
      @Nullable
      public Integer doInConnection(Connection conn) throws SQLException, DataAccessException {
        TableDefinition tdf = update_request.getDefinition();
        PreparedStatement ps = sql_dialect_provider.prepareDataDelete(conn, update_request);
        try {
          int i = 1;
          if (update_request.hasWhere()) {
            List<Object> where_conditions = new LinkedList<>();
            for (WhereCloumn w : update_request.getWhere()) {
              w.buildParameters(where_conditions, type_converter_factory);
            }
            for (Object where_object : where_conditions) {
              ps.setObject(i, where_object);
              i = i + 1;
            }
          } else {
            Object keyvalue = update_request.getInput_key();
            Object rowkey = update_request.getRow_key();
            if (update_request.testComposite_key()) {
              List<?> row_keys = (List<?>)rowkey;
              List<?> key_values = (List<?>)keyvalue;
              for (int j = 0; j < row_keys.size(); j++) {
                String keyname = (String)row_keys.get(j);
                Object keyval = key_values.get(j);
                TableColumn keycol = update_request.findColumn(keyname);
                ps.setObject(i, type_converter_factory.convertObject(
                  keyval, keycol != null ? keycol.getJdbc_type() : null
                ));
                i = i + 1;
              }
            } else {
              TableColumn keycol = update_request.findColumn((String)rowkey);
              ps.setObject(i, type_converter_factory.convertObject(
                keyvalue, keycol != null ? keycol.getJdbc_type() : null
              ));
              i = i + 1;
            }
          }

          if (tdf != null && tdf.hasOwner_id_column()) {
            if (loggedUserContext.isUserLogged()) {
              UserInfo logged_user = loggedUserContext.getLoginUserInfo();
              ps.setObject(i, logged_user.getIdByType(tdf.getOwner_id_column().getIdtype()));
            } else {
              ps.setObject(i, null);
            }
            i = i + 1;
          }

          if (tdf != null && tdf.hasDefault_where()) {
            List<Object> where_conditions = new LinkedList<>();
            List<WhereCloumn> copied_where = tdf.copyDefault_where();
            for (WhereCloumn w : copied_where) {
              w.buildParameters(where_conditions, type_converter_factory);
            }
            for (Object where_object : where_conditions) {
              ps.setObject(i, where_object);
              if (log.isDebugEnabled()) {
                log.debug("设置缺省定义条件参数: PI={}, WHERE={}", i, where_object);
              }
              i = i + 1;
            }
          }

          return ps.executeUpdate();
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
    Object result = doInsertTableData(
      insert_request,
      sql_dialect_provider,
      this.getTypeConverterFactory()
    );
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
    int []result = doUpdateTableData(
      update_request,
      sql_dialect_provider,
      this.getTypeConverterFactory()
    );
    if (update_request.testSignletonData()) {
      return Array.get(result, 0);
    } else {
      return result;
    }
  }

  @Override
  public Object deleteTableData(
    ManipulationRequest delete_request
  ) throws SQLException {
    SQLDialectProvider sql_dialect_provider = getSQLDialectProvider();
    Validate.notNull(sql_dialect_provider, "抱歉，暂时不支持%s!", getMetaData().getProduct_name());
    Integer result = doDeleteTableData(
      delete_request,
      sql_dialect_provider,
      this.getTypeConverterFactory()
    );
    return result;
  }

  protected void validateDMLRequest(JdbcDMLRequest []dmls, SQLDialectProvider sql_dialect_provider) throws SQLException {
    Map<String, TableMeta> table_metas = new HashMap<String, TableMeta>();
    for (int i = 0; i < dmls.length; i++) {
      JdbcDMLRequest dml = dmls[0];
      if (dml.getInsert() != null && dml.getInsert().length > 0) {
        for (int j = 0; j < dml.getInsert().length; j++) {
          ManipulationRequest insert_request = dml.getInsert()[j];
          String table_name = insert_request.getTable_name();
          Validate.notBlank(table_name, "[%d-%d]插入请求未设置表名", i, j);
          TableMeta table_meta = table_metas.get(table_name);
          if (table_meta == null) {
            table_metas.put(table_name, table_meta = queryResultMeta(JdbcQueryRequest.of(table_name)));
          }
          insert_request.copyTableMeta(table_meta);
          insert_request.rebuildWhere();
        }
      }
      if (dml.getUpdate() != null && dml.getUpdate().length > 0) {
        for (int j = 0; j < dml.getUpdate().length; j++) {
          ManipulationRequest update_request = dml.getUpdate()[j];
          String table_name = update_request.getTable_name();
          Validate.notBlank(table_name, "[%d-%d]更新请求未设置表名", i, j);
          Validate.isTrue(update_request.hasWhere(), "[%d-%d]更新请求未设置Where条件", i, j);
          TableMeta table_meta = table_metas.get(table_name);
          if (table_meta == null) {
            table_metas.put(table_name, table_meta = queryResultMeta(JdbcQueryRequest.of(table_name)));
          }
          update_request.copyTableMeta(table_meta);
          update_request.rebuildWhere();
        }
      }
      if (dml.getDelete() != null && dml.getDelete().length > 0) {
        for (int j = 0; j < dml.getDelete().length; j++) {
          ManipulationRequest delete_request = dml.getDelete()[j];
          String table_name = delete_request.getTable_name();
          Validate.notBlank(table_name, "[%d-%d]删除请求未设置表名", i, j);
          Validate.isTrue(delete_request.hasWhere(), "[%d-%d]删除请求未设置Where条件", i, j);
          TableMeta table_meta = table_metas.get(table_name);
          if (table_meta == null) {
            table_metas.put(table_name, table_meta = queryResultMeta(JdbcQueryRequest.of(table_name)));
          }
          delete_request.copyTableMeta(table_meta);
          delete_request.rebuildWhere();
        }
      }
    }
  }

  @Override
  public List<JdbcDMLResponse> executeDMLRequest(JdbcDMLRequest []dmls) throws SQLException {
    SQLDialectProvider sql_dialect_provider = getSQLDialectProvider();
    Validate.notNull(sql_dialect_provider, "抱歉，暂时不支持%s!", getMetaData().getProduct_name());
    validateDMLRequest(dmls, sql_dialect_provider);
    List<JdbcDMLResponse> responses = new ArrayList<JdbcDMLResponse>(dmls.length);
    for (int i = 0; i < dmls.length; i++) {
      JdbcDMLRequest dml = dmls[0];

      if ((dml.getInsert() == null || dml.getInsert().length == 0) &&
        (dml.getUpdate() == null || dml.getUpdate().length == 0) &&
        (dml.getDelete() == null || dml.getDelete().length == 0)
      ) continue;

      // 添加应答
      JdbcDMLResponse response = new JdbcDMLResponse();

      // 插入操作
      if (dml.getInsert() != null && dml.getInsert().length > 0) {
        for (int j = 0; j < dml.getInsert().length; j++) {
          ManipulationRequest insert_request = dml.getInsert()[j];
          try {
            Object result = doInsertTableData(
              insert_request,
              sql_dialect_provider,
              this.getTypeConverterFactory()
            );
            if (insert_request.testSignletonData()) {
              if (insert_request.hasAutoGenerated()) {
                result = new Object[] {
                  Array.get(Array.get(result, 0), 0),
                  Array.get(Array.get(result, 1), 0),
                };
              } else {
                result = Array.get(result, 0);
              }
            }
            response.addInsert(result);
          } catch (Throwable e) {
            throw new IllegalStateException(
              String.format("[%d-%d]插入请求失败: %s", i, j, e.getMessage()
            ), e);
          }
        }
      }

      // 更新操作
      if (dml.getUpdate() != null && dml.getUpdate().length > 0) {
        for (int j = 0; j < dml.getUpdate().length; j++) {
          ManipulationRequest update_request = dml.getUpdate()[j];
          try {
            Object result = doUpdateTableData(
              update_request,
              sql_dialect_provider,
              this.getTypeConverterFactory()
            );
            if (update_request.testSignletonData()) {
              result = Array.get(result, 0);
            }
            response.addUpdate(result);
          } catch (Throwable e) {
            throw new IllegalStateException(
              String.format("[%d-%d]更新请求失败: %s", i, j, e.getMessage()
            ), e);
          }
        }
      }

      // 删除操作
      if (dml.getDelete() != null && dml.getDelete().length > 0) {
        for (int j = 0; j < dml.getDelete().length; j++) {
          ManipulationRequest update_request = dml.getDelete()[j];
          try {
            Object result = doDeleteTableData(
              update_request,
              sql_dialect_provider,
              this.getTypeConverterFactory()
            );
            if (update_request.testSignletonData()) {
              result = Array.get(result, 0);
            }
            response.addDelete(result);
          } catch (Throwable e) {
            throw new IllegalStateException(
              String.format("[%d-%d]删除请求失败: %s", i, j, e.getMessage()
            ), e);
          }
        }
      }
      responses.add(response);
    }
    return responses;
  }

  private void doPrepareSQLServiceInputParameters(SQLServiceRequest sql_request, Map<String, Object> input_wrap) {
    if (loggedUserContext.isUserLogged()) {
      input_wrap.put("user", loggedUserContext.getLoginUserInfo());
    }
    Map<String, Object> request = new HashMap<>(2);
    input_wrap.put("req", request);
    request.put("time", sql_request.getRequest_time());
    request.put("client_ip", WebUtils.getClientRealIp());
  }

  @Override
  public Object executeSQLService(SQLServiceRequest sql_request) {
    SqlSession session = this.sessionFactory.openSession();
    List<Object> output_list = new LinkedList<>();
    try {
      for (Map<String, Object> input_data : sql_request.getInput_list()) {
        Object last_output = null;
        for (SQLServiceDefinition.SQLFragment sql_fragment : sql_request.getDefinition().getSql_fragments()) {
          Map<String, Object> input_wrap = new HashMap<String, Object>(2);
          this.doPrepareSQLServiceInputParameters(sql_request, input_wrap);
          input_wrap.put("input", input_data);
          if (last_output != null) {
            input_wrap.put("lastout", last_output);
          }
          if (output_list.size() > 0) {
            input_wrap.put("output", output_list);
          }
          last_output = doExecuteSQLService(session, sql_fragment, input_wrap);
        }
        output_list.add(last_output);
      }
    } finally {
      session.close();
    }
    if (sql_request.testSignletonData()) {
      if (output_list.size() > 0) {
        return output_list.get(0);
      } else {
        return null;
      }
    } else {
      return output_list;
    }
  }

  public Object doExecuteSQLService(
    SqlSession session,
    SQLServiceDefinition.SQLFragment sql_fragment,
    Object input_data
  ) {
    resolveMappedStatement(sql_fragment);
    SqlCommandType sql_cmd_type = sql_fragment.getCommand_type();
    if (sql_cmd_type == null || Objects.equals(SqlCommandType.UNKNOWN, sql_cmd_type)) {
      throw new IllegalStateException("无效SQL语句");
    }

    try {
      switch(sql_cmd_type) {
      case SELECT:
        return doSQLServiceSelect(session, sql_fragment, input_data);
      case INSERT:
        return doSQLServiceInsert(session, sql_fragment, input_data);
      case UPDATE:
        return doSQLServiceUpdate(session, sql_fragment, input_data);
      case DELETE:
        return doSQLServiceDelete(session, sql_fragment, input_data);
      default:
        throw new IllegalStateException("无效SQL语句");
      }
    } finally {
    }
  }

  private Object doSQLServiceInsert(
    SqlSession session,
    SQLServiceDefinition.SQLFragment sql_fragment,
    Object input_data
  ) {
    String mapped_id = sql_fragment.getMapped_id();
    long start_time = System.currentTimeMillis();
    try {
      return session.insert(mapped_id, input_data);
    } finally {
      log.debug("执行{}插入耗时{}毫秒", mapped_id, (System.currentTimeMillis() - start_time));
    }
  }

  private Object doSQLServiceUpdate(
    SqlSession session,
    SQLServiceDefinition.SQLFragment sql_fragment,
    Object input_data
  ) {
    String mapped_id = sql_fragment.getMapped_id();
    long start_time = System.currentTimeMillis();
    try {
      return session.update(mapped_id, input_data);
    } finally {
      log.debug("执行{}更新耗时{}毫秒", mapped_id, (System.currentTimeMillis() - start_time));
    }
  }

  private Object doSQLServiceDelete(
    SqlSession session,
    SQLServiceDefinition.SQLFragment sql_fragment,
    Object input_data
  ) {
    String mapped_id = sql_fragment.getMapped_id();
    long start_time = System.currentTimeMillis();
    try {
      return session.delete(mapped_id, input_data);
    } finally {
      log.debug("执行{}删除耗时{}毫秒", mapped_id, (System.currentTimeMillis() - start_time));
    }
  }

  @SuppressWarnings("unchecked")
  private Object doSQLServiceSelect(
    SqlSession session,
    SQLServiceDefinition.SQLFragment sql_fragment,
    Object input_data
  ) {
    String mapped_id = sql_fragment.getMapped_id();
    long start_time = System.currentTimeMillis();
    try {
      List<Object> result = session.selectList(mapped_id, input_data);
      if (sql_fragment.getResult().isSignleton()) {
        if (result == null || result.size() == 0) {
          return null;
        }
        Map<String, Object> map = (Map<String, Object>)result.get(0);
        if (sql_fragment.getResult().isColumn_compact()) {
          return map.get(map.keySet().iterator().next());
        } else {
          return map;
        }
      } else {
        return result;
      }
    } finally {
      log.debug("执行{}查询耗时{}毫秒", mapped_id, (System.currentTimeMillis() - start_time));
    }
  }

  private String resolveMappedStatement(
    SQLServiceDefinition.SQLFragment sql_fragment
  ) {
    long start_time = System.currentTimeMillis();
    try {
      Configuration mybatis_config = this.sessionFactory.getConfiguration();
      if (mybatis_config.hasStatement(sql_fragment.getMapped_id(), false)) {
        return sql_fragment.getMapped_id();
      }
      mybatis_config.addMappedStatement(sql_fragment.getMapped_statement());
      return sql_fragment.getMapped_id();
    } finally {
      if (log.isDebugEnabled()) {
        log.debug("构建{}耗时{}毫秒", sql_fragment.getMapped_id(), (System.currentTimeMillis() - start_time));
      }
    }
  }

}
