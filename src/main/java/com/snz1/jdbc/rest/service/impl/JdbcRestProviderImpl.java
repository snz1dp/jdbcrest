package com.snz1.jdbc.rest.service.impl;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import javax.annotation.Resource;

import org.apache.commons.beanutils.BeanUtilsBean;
import org.apache.commons.beanutils.PropertyUtilsBean;
import org.apache.commons.lang3.Validate;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.springframework.beans.factory.InitializingBean;
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
import com.snz1.jdbc.rest.provider.AbstractSQLDialectProvider;
import com.snz1.jdbc.rest.provider.SQLDialectProvider;
import com.snz1.jdbc.rest.data.TableDefinition;
import com.snz1.jdbc.rest.data.JdbcQueryRequest;
import com.snz1.jdbc.rest.service.AppInfoResolver;
import com.snz1.jdbc.rest.service.CacheClear;
import com.snz1.jdbc.rest.service.JdbcRestProvider;
import com.snz1.jdbc.rest.service.JdbcTypeConverterFactory;
import com.snz1.jdbc.rest.service.LoggedUserContext;
import com.snz1.jdbc.rest.service.TableDefinitionRegistry;
import com.snz1.jdbc.rest.service.UserRoleVerifier;
import com.snz1.jdbc.rest.service.converter.ListConverter;
import com.snz1.jdbc.rest.service.converter.MapConverter;
import com.snz1.jdbc.rest.service.converter.PropertiesConverter;
import com.snz1.jdbc.rest.service.converter.SetConverter;
import com.snz1.jdbc.rest.service.converter.StringLocaleConverter;
import com.snz1.jdbc.rest.utils.JdbcUtils;
import com.snz1.jdbc.rest.utils.ObjectUtils;
import com.snz1.utils.WebUtils;

import org.apache.ibatis.mapping.SqlCommandType;

import gateway.api.NotFoundException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcRestProviderImpl implements JdbcRestProvider, CacheClear, InitializingBean {

  protected JdbcMetaData jdbcMetaData;

  @Resource
  protected JdbcTemplate jdbcTemplate;

  @Resource
  protected SqlSessionFactory sessionFactory;

  @Resource
  protected TableDefinitionRegistry tableDefinitionRegistry;

  @Resource
  protected LoggedUserContext loggedUserContext;

  @Resource
  protected JdbcTypeConverterFactory typeConverterFactory;

  @Resource
  protected AppInfoResolver appInfoResolver;

  @Resource
  private UserRoleVerifier userRoleVerifier;

  private BeanUtilsBean beanUtilsBean = new BeanUtilsBean(new ConvertUtilsBean(), new PropertyUtilsBean());

  @Override
  public BeanUtilsBean getBeanUtils() {
    return beanUtilsBean;
  }

  public JdbcRestProviderImpl() {
    this.getBeanUtils().getConvertUtils().register(new StringLocaleConverter(), String.class);
    this.getBeanUtils().getConvertUtils().register(new MapConverter(), Map.class);
    this.getBeanUtils().getConvertUtils().register(new PropertiesConverter(), Properties.class);
    this.getBeanUtils().getConvertUtils().register(new ListConverter(), List.class);
    this.getBeanUtils().getConvertUtils().register(new SetConverter(), Set.class);
  }

  @Override
  public void afterPropertiesSet() {
    AbstractSQLDialectProvider provider;
    try {
      provider = (AbstractSQLDialectProvider) getSQLDialectProvider();
      provider.setLoggedUserContext(loggedUserContext);
      provider.setTypeConverterFactory(typeConverterFactory);
      provider.setUserRoleVerifier(userRoleVerifier);
    } catch (SQLException e) {
      throw new IllegalStateException("初始话数据库提供器失败");
    }
  }

  public JdbcTypeConverterFactory getTypeConverterFactory() {
    return this.typeConverterFactory;
  }

  protected AppInfoResolver getAppInfoResolver() {
    return appInfoResolver;
  }

  protected LoggedUserContext getLoggedUserContext() {
    return loggedUserContext;
  }

  // 获取Schemas
  public JdbcQueryResponse<List<Object>> getSchemas() throws SQLException {
    return jdbcTemplate.execute(new GetSchemasHandler(this.appInfoResolver, this.beanUtilsBean));
  }

  // 获取目录
  public JdbcQueryResponse<List<Object>> getCatalogs() throws SQLException {
    return jdbcTemplate.execute(new GetCatalogsHandler(this.appInfoResolver, this.beanUtilsBean));
  }

  // 获取数据库元信息
  @Override
  public JdbcMetaData getMetaData() throws SQLException {
    if (this.jdbcMetaData != null) return this.jdbcMetaData;
    return this.jdbcMetaData = jdbcTemplate.execute(new GetJdbcMetaHandler(this.appInfoResolver, this.beanUtilsBean));
  }

  // 获取表类表
  @Override
  public JdbcQueryResponse<Page<Object>> getTables(
    ResultDefinition return_meta,
    String catalog, String schema_pattern,
    String table_name_pattern, Long offset,
    Long limit, String...types
  ) throws SQLException {
    if (log.isDebugEnabled()) {
      log.debug("获取数据表列表(CATALOG={}, SCHEMA={}, TABLE={}, TYPES={})...",
        catalog, schema_pattern, table_name_pattern, types);
    }
    long start_time = System.currentTimeMillis();
    try {
      return jdbcTemplate.execute(new GetTablesHandler(
        this.appInfoResolver, this.beanUtilsBean, return_meta, catalog,
        schema_pattern, table_name_pattern,
        offset, limit, types
      ));
    } finally {
      if (log.isDebugEnabled()) {
        log.debug("获取数据表列表(CATALOG={}, SCHEMA={}, TABLE={}, TYPES={}耗时{}毫秒",
          catalog, schema_pattern, table_name_pattern, types, (System.currentTimeMillis() - start_time)
        );
      }
    }
  }

  // 测试表是否存在
  protected boolean doTestTableExisted(SQLDialectProvider sql_dialect_provider,
    String catalog_name, String schema_name, String table_name, String ...types) {
    if (log.isDebugEnabled()) {
      log.debug("检查数据表{}是否存在...", table_name);
    }
    long start_time = System.currentTimeMillis();
    try {
      return Objects.equals(Boolean.TRUE, jdbcTemplate.execute(new TestTableExistedHandler(
        sql_dialect_provider, this.appInfoResolver, this.beanUtilsBean, catalog_name, schema_name, table_name, types
      )));
    } finally {
      if (log.isDebugEnabled()) {
        log.debug("检查数据表{}是否存在耗时{}毫秒", table_name, (System.currentTimeMillis() - start_time));
      }
    }
  }

  // 执行获取结果集元信息
  protected TableMeta doFetchResultSetMeta(final JdbcQueryRequest table_query, final SQLDialectProvider sql_dialect_provider) {
    if (table_query.hasTable_meta()) {
      return table_query.getTable_meta();
    }
    return jdbcTemplate.execute(new TableMetaRequestHandler(table_query, sql_dialect_provider, this.appInfoResolver, this.beanUtilsBean));
  }

  // 元信息
  public TableMeta queryResultMeta(
    JdbcQueryRequest table_query
  ) throws SQLException {
    if (table_query.hasTable_meta()) return table_query.getTable_meta();
    SQLDialectProvider sql_dialect_provider = getSQLDialectProvider();
    Validate.notNull(sql_dialect_provider, "暂时不支持%s数据库", getMetaData().getProduct_name());
    if (tableDefinitionRegistry.hasTableDefinition(table_query.getTable_name())) {
      TableDefinition definition = tableDefinitionRegistry.getTableDefinition(table_query.getTable_name());
      table_query.setDefinition(definition);
    } else if (this.appInfoResolver.isStrictMode()) {
      throw new NotFoundException(String.format("%s不存在", table_query.getFullTableName()));
    }

    if (sql_dialect_provider.checkTableExisted() && !doTestTableExisted(
      sql_dialect_provider, table_query.getCatalog_name(),
      table_query.getSchema_name(), table_query.getTable_name())) {
      throw new NotFoundException(String.format("%s不存在", table_query.getFullTableName()));
    }

    return doFetchResultSetMeta(table_query, sql_dialect_provider);
  }

  // 分页查询统计信息
  protected boolean doFetchQueryPageTotal(JdbcQueryRequest table_query, SQLDialectProvider sql_dialect_provider, JdbcQueryResponse<Page<Object>> pageret) {
    if (log.isDebugEnabled()) {
      log.debug("执行表{}分页查询统计...", table_query.getFullTableName());
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
        log.debug("执行表{}分页查询统计耗时{}毫秒", table_query.getFullTableName(), (System.currentTimeMillis() - start_time));
      }
    }
  }

  protected Long doQueryTotalResult(JdbcQueryRequest table_query, SQLDialectProvider sql_dialect_provider) {
    long start_time = System.currentTimeMillis();
    if (log.isDebugEnabled()) {
      log.debug("执行表{}数据行统计...", table_query.getFullTableName());
    }
    try {
      // 获取统计
      Long query_count = new TotalQueryRequestHandler(table_query, this.jdbcTemplate, sql_dialect_provider).execute();
      return query_count;
    } finally {
      if (log.isDebugEnabled()) {
        log.debug("执行表{}数据行统计耗时{}毫秒", table_query.getFullTableName(), (System.currentTimeMillis() - start_time));
      }
    }
  }

  // 查询列表结果
  protected JdbcQueryResponse<?> doQueryListResult(JdbcQueryRequest table_query, SQLDialectProvider sql_dialect_provider) {
    long start_time = System.currentTimeMillis();
    if (log.isDebugEnabled()) {
      log.debug("执行表{}数据行查询...", table_query.getFullTableName());
    }
    try {
      // 获取列表数据
      JdbcQueryResponse<List<Object>> datalist = jdbcTemplate.execute(new ListQueryRequestHandler(
        table_query, sql_dialect_provider, this.appInfoResolver, this.beanUtilsBean
      ));
      return datalist;
    } finally {
      if (log.isDebugEnabled()) {
        log.debug("执行表{}数据行查询耗时{}毫秒", table_query.getFullTableName(), (System.currentTimeMillis() - start_time));
      }
    }
  }

  @Override
  @SuppressWarnings({"rawtypes", "unchecked"})
  public JdbcQueryResponse<?> queryPageResult(
    JdbcQueryRequest table_query
  ) throws SQLException {
    SQLDialectProvider sql_dialect_provider = getSQLDialectProvider();
    Validate.notNull(sql_dialect_provider, "暂时不支持%s数据库", getMetaData().getProduct_name());

    if (!table_query.hasTable_meta()) { // 无表元信息则查询
      if (sql_dialect_provider.checkTableExisted() && !doTestTableExisted(
        sql_dialect_provider, table_query.getCatalog_name(),
        table_query.getSchema_name(), table_query.getTable_name())) {
        throw new NotFoundException(String.format("数据表%s不存在", table_query.getFullTableName()));
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
    pageret.setLic(datalist.getLic());

    // 返回数据
    if (datalist != null && datalist.getData() instanceof List) {
      pageret.setMeta(datalist.getMeta());
      pageret.getData().setOffset(table_query.getResult().getOffset());
      pageret.getData().setData((List)datalist.getData());
    }
    return pageret;
  }

  protected SQLDialectProvider getSQLDialectProvider() throws SQLException {
    JdbcMetaData metadata = getMetaData();
    return JdbcUtils.getSQLDialectProvider(metadata.getDriver_id());
  }

  @Override
  @SuppressWarnings("unchecked")
  public JdbcQueryResponse<Long> queryAllCountResult(JdbcQueryRequest table_query) throws SQLException {
    SQLDialectProvider sql_dialect_provider = getSQLDialectProvider();
    Validate.notNull(sql_dialect_provider, "暂时不支持%s数据库", getMetaData().getProduct_name());
    table_query.getResult().setRow_struct(ResultDefinition.ResultRowStruct.list);
    table_query.getResult().setOffset(0l);
    table_query.getResult().setLimit(1l);
    JdbcQueryResponse<?> datalist = this.doQueryListResult(table_query, sql_dialect_provider);
    Object total_val = ((List<Object>)(((List<Object>)datalist.getData()).get(0))).get(0);
    JdbcQueryResponse<Long> ret = new JdbcQueryResponse<>();
    if (total_val instanceof BigDecimal) {
      ret.setData(((BigDecimal)total_val).longValue());
    } else if (total_val instanceof BigInteger) {
      ret.setData(((BigInteger)total_val).longValue());
    } else if (total_val instanceof Integer) {
      ret.setData(((Integer)total_val).longValue());
    } else if (total_val instanceof Long) {
      ret.setData((Long)total_val);
    }
    ret.setLic(datalist.getLic());
    return ret;
  }

  @Override
  public JdbcQueryResponse<?> queryGroupCountResult(JdbcQueryRequest table_query) throws SQLException {
    SQLDialectProvider sql_dialect_provider = getSQLDialectProvider();
    Validate.notNull(sql_dialect_provider, "暂时不支持%s数据库", getMetaData().getProduct_name());
    table_query.getResult().setLimit(Constants.DEFAULT_MAX_LIMIT);
    JdbcQueryResponse<?> datalist = this.doQueryListResult(table_query, sql_dialect_provider);
    return datalist;
  }

  @Override
  public JdbcQueryResponse<?> queryGroupResult(JdbcQueryRequest table_query) throws SQLException {
    SQLDialectProvider sql_dialect_provider = getSQLDialectProvider();
    Validate.notNull(sql_dialect_provider, "暂时不支持%s数据库", getMetaData().getProduct_name());
    table_query.getResult().setLimit(Constants.DEFAULT_MAX_LIMIT);
    JdbcQueryResponse<?> datalist = this.doQueryListResult(table_query, sql_dialect_provider);
    return datalist;
  }

  @Override
  @SuppressWarnings("unchecked")
  public JdbcQueryResponse<?> querySignletonResult(JdbcQueryRequest table_query) throws SQLException {
    SQLDialectProvider sql_dialect_provider = getSQLDialectProvider();
    Validate.notNull(sql_dialect_provider, "暂时不支持%s数据库", getMetaData().getProduct_name());
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
    ret.setLic(datalist.getLic());
    return ret;
  }

  protected Object doInsertTableData(
    ManipulationRequest insert_request,
    SQLDialectProvider sql_dialect_provider,
    JdbcTypeConverterFactory type_converter_factory
  ) throws SQLException {
    return jdbcTemplate.execute(new InsertRequestHandler(
      insert_request, sql_dialect_provider,
      type_converter_factory, this.loggedUserContext,
      this.appInfoResolver, this.beanUtilsBean
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
      type_converter_factory, this.loggedUserContext,
      this.appInfoResolver, this.beanUtilsBean
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
      type_converter_factory, this.loggedUserContext,
      this.appInfoResolver, this.beanUtilsBean
    ));
  }

  // 插入表数据
  @Override
  public Object insertTableData(
    ManipulationRequest insert_request
  ) throws SQLException {
    SQLDialectProvider sql_dialect_provider = getSQLDialectProvider();
    Validate.notNull(sql_dialect_provider, "暂时不支持%s数据库", getMetaData().getProduct_name());
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
    Validate.notNull(sql_dialect_provider, "暂时不支持%s数据库", getMetaData().getProduct_name());
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
    Validate.notNull(sql_dialect_provider, "暂时不支持%s数据库", getMetaData().getProduct_name());
    Integer result = doDeleteTableData(
      delete_request,
      sql_dialect_provider,
      this.getTypeConverterFactory()
    );
    return result;
  }

  private void doValidateDMLRequest(JdbcDMLRequest []dmls, SQLDialectProvider sql_dialect_provider) throws SQLException {
    Map<String, TableMeta> table_metas = new HashMap<String, TableMeta>();
    for (int i = 0; i < dmls.length; i++) {
      JdbcDMLRequest dml = dmls[0];
      if (dml.getInsert() != null && dml.getInsert().length > 0) {
        for (int j = 0; j < dml.getInsert().length; j++) {
          ManipulationRequest insert_request = dml.getInsert()[j];
          String catalog_name = insert_request.getCatalog_name();
          String schema_name = insert_request.getSchema_name();
          String table_name = insert_request.getTable_name();
          Validate.notBlank(table_name, "[%d-%d]插入请求未设置表名", i, j);
          TableMeta table_meta = table_metas.get(insert_request.getFullTableName());
          if (table_meta == null) {
            table_metas.put(insert_request.getFullTableName(),
              table_meta = queryResultMeta(JdbcQueryRequest.of(
                catalog_name, schema_name, table_name)));
          }
          insert_request.copyTableMeta(table_meta);
          insert_request.rebuildWhere();
        }
      }
      if (dml.getUpdate() != null && dml.getUpdate().length > 0) {
        for (int j = 0; j < dml.getUpdate().length; j++) {
          ManipulationRequest update_request = dml.getUpdate()[j];
          String catalog_name = update_request.getCatalog_name();
          String schema_name = update_request.getSchema_name();
          String table_name = update_request.getTable_name();
          Validate.notBlank(table_name, "[%d-%d]更新请求未设置表名", i, j);
          Validate.isTrue(update_request.hasWhere(), "[%d-%d]更新请求未设置Where条件", i, j);
          TableMeta table_meta = table_metas.get(update_request.getFullTableName());
          if (table_meta == null) {
            table_metas.put(update_request.getFullTableName(),
              table_meta = queryResultMeta(JdbcQueryRequest.of(
                catalog_name, schema_name, table_name)));
          }
          update_request.copyTableMeta(table_meta);
          update_request.rebuildWhere();
        }
      }
      if (dml.getDelete() != null && dml.getDelete().length > 0) {
        for (int j = 0; j < dml.getDelete().length; j++) {
          ManipulationRequest delete_request = dml.getDelete()[j];
          String catalog_name = delete_request.getCatalog_name();
          String schema_name = delete_request.getSchema_name();
          String table_name = delete_request.getTable_name();
          Validate.notBlank(table_name, "[%d-%d]删除请求未设置表名", i, j);
          Validate.isTrue(delete_request.hasWhere(), "[%d-%d]删除请求未设置Where条件", i, j);
          TableMeta table_meta = table_metas.get(delete_request.getFullTableName());
          if (table_meta == null) {
            table_metas.put(delete_request.getFullTableName(),
              table_meta = queryResultMeta(JdbcQueryRequest.of(
                catalog_name, schema_name, table_name)));
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
    Validate.notNull(sql_dialect_provider, "暂时不支持%s数据库", getMetaData().getProduct_name());
    doValidateDMLRequest(dmls, sql_dialect_provider);
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
      input_wrap.put("user", loggedUserContext.getLoggedUser());
    }
    Map<String, Object> request = new HashMap<>(3);
    request.put("time", sql_request.getRequest_time());
    request.put("client_ip", WebUtils.getClientRealIp());
    request.put("user_agent", WebUtils.getClientUserAgent());
    input_wrap.put("req", request);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T executeSQLService(SQLServiceRequest sql_request) {
    SqlSession session = this.sessionFactory.openSession();
    List<T> output_list = new LinkedList<>();
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
        output_list.add((T)last_output);
      }
    } finally {
      session.close();
    }
    if (sql_request.testSignletonData()) {
      if (output_list.size() > 0) {
        return output_list.get(output_list.size() - 1);
      } else {
        return null;
      }
    } else {
      return (T)output_list;
    }
  }

  @SuppressWarnings("unchecked")
  public <T> T doExecuteSQLService(
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
        return (T)doSQLServiceInsert(session, sql_request, sql_fragment, input_data);
      case UPDATE:
        return (T)doSQLServiceUpdate(session, sql_request, sql_fragment, input_data);
      case DELETE:
        return (T)doSQLServiceDelete(session, sql_request, sql_fragment, input_data);
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
  protected <T> T doSQLServiceSelect(
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
          return (T)map.get(map.keySet().iterator().next());
        } else if (sql_request.getResult_class() == null ||
          Objects.equals(sql_request.getResult_class(), Map.class)
        ) {
          return (T)map;
        } else {
          try {
            return (T)ObjectUtils.mapToObject(this.getBeanUtils(), map, sql_request.getResult_class().getDeclaredConstructor().newInstance());
          } catch (Throwable e) {
            throw new IllegalStateException("类型转换失败：" + e.getMessage(), e);
          }
        }
      } else if (sql_request.getResult_class() == null ||
        Objects.equals(sql_request.getResult_class(), Map.class)
      ) {
        return (T)result;
      } else {
        List<T> list = new ArrayList<T>(result.size());
        for (Object obj : result) {
          try {
            list.add((T)ObjectUtils.mapToObject(this.getBeanUtils(), (Map<String, Object>)obj, sql_request.getResult_class().getDeclaredConstructor().newInstance()));
          } catch (Throwable e) {
            throw new IllegalStateException("类型转换失败：" + e.getMessage(), e);
          }
        }
        return (T)list;
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

  @Override
  public void clearServiceCaches() {
  }

  @Override
  public void clearTableCaches() {
  }

  @Override
  public void clearCaches() {
    try {
      this.clearTableCaches();
    } finally {
      this.clearServiceCaches();
      this.clearMetaCaches();
    }
  }

}
