package com.snz1.jdbc.rest.service.impl;

import java.lang.reflect.Array;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Resource;

import org.apache.commons.lang3.Validate;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.JdbcTemplate;

import com.snz1.jdbc.rest.Constants;
import com.snz1.jdbc.rest.data.ManipulationRequest;
import com.snz1.jdbc.rest.data.JdbcDMLRequest;
import com.snz1.jdbc.rest.data.JdbcDMLResponse;
import com.snz1.jdbc.rest.data.JdbcMetaData;
import com.snz1.jdbc.rest.data.JdbcQueryResponse;
import com.snz1.jdbc.rest.data.Page;
import com.snz1.jdbc.rest.data.ResultDefinition;
import com.snz1.jdbc.rest.data.SQLServiceDefinition;
import com.snz1.jdbc.rest.data.SQLServiceRequest;
import com.snz1.jdbc.rest.data.TableMeta;
import com.snz1.jdbc.rest.data.TableDefinition;
import com.snz1.jdbc.rest.data.JdbcQueryRequest;
import com.snz1.jdbc.rest.service.JdbcRestProvider;
import com.snz1.jdbc.rest.service.JdbcTypeConverterFactory;
import com.snz1.jdbc.rest.service.LoggedUserContext;
import com.snz1.jdbc.rest.service.SQLDialectProvider;
import com.snz1.jdbc.rest.service.TableDefinitionRegistry;
import com.snz1.utils.WebUtils;

import org.apache.ibatis.mapping.SqlCommandType;

import gateway.api.NotFoundException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
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

  // 获取Schemas
  public JdbcQueryResponse<List<Object>> getSchemas() throws SQLException {
    return jdbcTemplate.execute(new GetSchemasHandler());
  }

  // 获取目录
  public JdbcQueryResponse<List<Object>> getCatalogs() throws SQLException {
    return jdbcTemplate.execute(new GetCatalogsHandler());
  }

  // 获取数据库元信息
  @Override
  public JdbcMetaData getMetaData() throws SQLException {
    if (this.jdbcMetaData != null) return this.jdbcMetaData;
    return this.jdbcMetaData = jdbcTemplate.execute(new GetJdbcMetaHandler());
  }

  // 获取表类表
  @Override
  public JdbcQueryResponse<List<Object>> getTables(
    ResultDefinition return_meta,
    String catalog, String schema_pattern,
    String table_name_pattern, String...types
  ) throws SQLException {
    if (log.isDebugEnabled()) {
      log.debug("获取数据表列表(CATALOG={}, SCHEMA={}, TABLE={}, TYPES={})...",
        catalog, schema_pattern, table_name_pattern, types);
    }
    long start_time = System.currentTimeMillis();
    try {
      return jdbcTemplate.execute(new GetTablesHandler(return_meta, catalog, schema_pattern, table_name_pattern, types));
    } finally {
      if (log.isDebugEnabled()) {
        log.debug("获取数据表列表(CATALOG={}, SCHEMA={}, TABLE={}, TYPES={}耗时{}毫秒",
          catalog, schema_pattern, table_name_pattern, types, (System.currentTimeMillis() - start_time)
        );
      }
    }
  }
  
  // 测试表是否存在
  public boolean testTableExisted(String table_name, String ...types) {
    if (log.isDebugEnabled()) {
      log.debug("检查数据表{}是否存在...", table_name);
    }
    long start_time = System.currentTimeMillis();
    try {
      return Objects.equals(Boolean.TRUE, jdbcTemplate.execute(new TestTableExistedHandler(table_name, types)));
    } finally {
      if (log.isDebugEnabled()) {
        log.debug("检查数据表{}是否存在耗时{}毫秒", table_name, (System.currentTimeMillis() - start_time));
      }
    }
  }

  // 获取主键
  @Override
  public Object getTablePrimaryKey(String table_name) throws SQLException {
    return jdbcTemplate.execute(new GetPrimaryKeyHandler(table_name));
  }

  // 执行获取结果集元信息
  protected TableMeta doFetchResultSetMeta(final JdbcQueryRequest table_query, final SQLDialectProvider sql_dialect_provider) {
    if (table_query.hasTable_meta()) {
      return table_query.getTable_meta();
    }
    return jdbcTemplate.execute(new TableMetaRequestHandler(table_query, sql_dialect_provider));
  }

  protected TableDefinitionRegistry getTableDefinitionRegistry() {
    return tableDefinitionRegistry;
  }

  protected String resolveRealTableName(String table_name) {
    if (tableDefinitionRegistry.hasTableDefinition(table_name)) {
      TableDefinition definition = tableDefinitionRegistry.getTableDefinition(table_name);
      if (definition.hasAlias_name()) {
        table_name = definition.getAlias();
      }
    }
    return table_name;
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
    if (log.isDebugEnabled()) {
      log.debug("执行表{}分页查询统计...", table_query.getTable_name());
    }
    long start_time = System.currentTimeMillis();
    try {
      // 获取统计
      Long query_count = this.doQueryTotalResult(table_query, sql_dialect_provider);

      // 设置配置
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
      if (log.isDebugEnabled()) {
        log.debug("执行表{}分页查询统计耗时{}毫秒", table_query.getTable_name(), (System.currentTimeMillis() - start_time));
      }
    }
  }

  protected Long doQueryTotalResult(JdbcQueryRequest table_query, SQLDialectProvider sql_dialect_provider) {
    long start_time = System.currentTimeMillis();
    if (log.isDebugEnabled()) {
      log.debug("执行表{}数据行统计...", table_query.getTable_name());
    }
    try {
      // 获取统计
      Long query_count = new TotalQueryRequestHandler(table_query, this.jdbcTemplate, sql_dialect_provider).execute();
      return query_count;
    } finally {
      if (log.isDebugEnabled()) {
        log.debug("执行表{}数据行统计耗时{}毫秒", table_query.getTable_name(), (System.currentTimeMillis() - start_time));
      }
    }
  }

  // 查询列表结果
  protected JdbcQueryResponse<?> doQueryListResult(JdbcQueryRequest table_query, SQLDialectProvider sql_dialect_provider) {
    long start_time = System.currentTimeMillis();
    if (log.isDebugEnabled()) {
      log.debug("执行表{}数据行查询...", table_query.getTable_name());
    }
    try {
      // 获取列表数据
      JdbcQueryResponse<List<Object>> datalist = jdbcTemplate.execute(new ListQueryRequestHandler(
        table_query, sql_dialect_provider
      ));
      return datalist;
    } finally {
      if (log.isDebugEnabled()) {
        log.debug("执行表{}数据行查询耗时{}毫秒", table_query.getTable_name(), (System.currentTimeMillis() - start_time));
      }
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

  protected Object doInsertTableData(
    ManipulationRequest insert_request,
    SQLDialectProvider sql_dialect_provider,
    JdbcTypeConverterFactory type_converter_factory
  ) throws SQLException {
    return jdbcTemplate.execute(new InsertRequestHandler(
      insert_request, sql_dialect_provider,
      type_converter_factory, this.loggedUserContext
    ));
  }

  // 执行更新
  protected int[] doUpdateTableData(
    ManipulationRequest update_request,
    SQLDialectProvider sql_dialect_provider,
    JdbcTypeConverterFactory type_converter_factory
  ) throws SQLException {
    return jdbcTemplate.execute(new UpdateRequestHandler(
      update_request, sql_dialect_provider,
      type_converter_factory, this.loggedUserContext
    ));
  }

  // 执行更新
  protected Integer doDeleteTableData(
    ManipulationRequest delete_request,
    SQLDialectProvider sql_dialect_provider,
    JdbcTypeConverterFactory type_converter_factory
  ) throws SQLException {
    return jdbcTemplate.execute(new DeleteRequestHandler(
      delete_request, sql_dialect_provider,
      type_converter_factory, this.loggedUserContext
    ));
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
          last_output = doExecuteSQLService(session, sql_request, sql_fragment, input_wrap);
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
    SQLServiceRequest sql_request,
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
        return doSQLServiceSelect(session, sql_request, sql_fragment, input_data);
      case INSERT:
        return doSQLServiceInsert(session, sql_request, sql_fragment, input_data);
      case UPDATE:
        return doSQLServiceUpdate(session, sql_request, sql_fragment, input_data);
      case DELETE:
        return doSQLServiceDelete(session, sql_request, sql_fragment, input_data);
      default:
        throw new IllegalStateException("无效SQL语句");
      }
    } finally {
    }
  }

  protected Object doSQLServiceInsert(
    SqlSession session,
    SQLServiceRequest sql_request,
    SQLServiceDefinition.SQLFragment sql_fragment,
    Object input_data
  ) {
    String mapped_id = sql_fragment.getMapped_id();
    if (log.isDebugEnabled()) {
      log.debug("执行{}插入请求...", mapped_id);
    }
    long start_time = System.currentTimeMillis();
    try {
      return session.insert(mapped_id, input_data);
    } finally {
      if (log.isDebugEnabled()) {
        log.debug("执行{}插入请求耗时{}毫秒", mapped_id, (System.currentTimeMillis() - start_time));
      }
    }
  }

  protected Object doSQLServiceUpdate(
    SqlSession session,
    SQLServiceRequest sql_request,
    SQLServiceDefinition.SQLFragment sql_fragment,
    Object input_data
  ) {
    String mapped_id = sql_fragment.getMapped_id();
    if (log.isDebugEnabled()) {
      log.debug("执行{}更新请求...", mapped_id);
    }
  long start_time = System.currentTimeMillis();
    try {
      return session.update(mapped_id, input_data);
    } finally {
      if (log.isDebugEnabled()) {
        log.debug("执行{}更新请求耗时{}毫秒", mapped_id, (System.currentTimeMillis() - start_time));
      }
    }
  }

  protected Object doSQLServiceDelete(
    SqlSession session,
    SQLServiceRequest sql_request,
    SQLServiceDefinition.SQLFragment sql_fragment,
    Object input_data
  ) {
    String mapped_id = sql_fragment.getMapped_id();
    if (log.isDebugEnabled()) {
      log.debug("执行{}删除请求...", mapped_id);
    }
    long start_time = System.currentTimeMillis();
    try {
      return session.delete(mapped_id, input_data);
    } finally {
      if (log.isDebugEnabled()) {
        log.debug("执行{}删除请求耗时{}毫秒", mapped_id, (System.currentTimeMillis() - start_time));
      }
    }
  }

  @SuppressWarnings("unchecked")
  protected Object doSQLServiceSelect(
    SqlSession session,
    SQLServiceRequest sql_request,
    SQLServiceDefinition.SQLFragment sql_fragment,
    Object input_data
  ) {
    String mapped_id = sql_fragment.getMapped_id();
    if (log.isDebugEnabled()) {
      log.debug("执行{}查询请求...", mapped_id);
    }
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
      if (log.isDebugEnabled()) {
        log.debug("执行{}查询请求耗时{}毫秒", mapped_id, (System.currentTimeMillis() - start_time));
      }
    }
  }

  private String resolveMappedStatement(
    SQLServiceDefinition.SQLFragment sql_fragment
  ) {
    if (log.isDebugEnabled()) {
      log.debug("构建{}请求...", sql_fragment.getMapped_id());
    }
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
        log.debug("构建{}请求耗时{}毫秒", sql_fragment.getMapped_id(), (System.currentTimeMillis() - start_time));
      }
    }
  }

  @Override
  public void clearTableCaches(String table_name) {
  }

  @Override
  public void clearMetaCaches() {
  }

  @Override
  public void clearServiceCaches(String service_path) {
  }

}
