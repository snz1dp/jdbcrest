package com.snz1.jdbc.rest.service.impl;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.session.SqlSession;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.Cache.ValueWrapper;

import com.snz1.jdbc.rest.Constants;
import com.snz1.jdbc.rest.data.JdbcMetaData;
import com.snz1.jdbc.rest.data.JdbcQueryRequest;
import com.snz1.jdbc.rest.data.JdbcQueryResponse;
import com.snz1.jdbc.rest.data.ManipulationRequest;
import com.snz1.jdbc.rest.data.Page;
import com.snz1.jdbc.rest.data.ResultDefinition;
import com.snz1.jdbc.rest.data.SQLServiceDefinition;
import com.snz1.jdbc.rest.data.SQLServiceRequest;
import com.snz1.jdbc.rest.data.TableMeta;
import com.snz1.jdbc.rest.data.SQLServiceDefinition.SQLFragment;
import com.snz1.jdbc.rest.provider.SQLDialectProvider;
import com.snz1.jdbc.rest.service.AppInfoResolver;
import com.snz1.jdbc.rest.service.JdbcTypeConverterFactory;
import com.snz1.jdbc.rest.service.SQLServiceRegistry;
import com.snz1.utils.JsonUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CachedJdbcRestProvider extends JdbcRestProviderImpl {
  
  @Resource
  private CacheManager cacheManager;

  @Resource
  private AppInfoResolver appInfoResolver;

  @Resource
  private SQLServiceRegistry serviceRegistry;

  private static ThreadLocal<Boolean> IGNORE_CACHE_HOLDER = new ThreadLocal<>();

  public CachedJdbcRestProvider() {
  }

  @Override
  public boolean isThreadCacheEnabled() {
    return IGNORE_CACHE_HOLDER.get() == null || !IGNORE_CACHE_HOLDER.get();
  }

  @Override
  public void setThreadCacheEnabled(boolean cache_enabled) {
    IGNORE_CACHE_HOLDER.set(!cache_enabled);
  }

  @SuppressWarnings("unchecked")
  private <T> T resolveCached(String cache_name, String cache_key, ValueResolver<T> resolver) throws SQLException {
    if (IGNORE_CACHE_HOLDER.get() != null && IGNORE_CACHE_HOLDER.get()) {
      return resolver.resolve();
    }
    Cache cache = cacheManager.getCache(cache_name);
    if (cache == null) {
      if (log.isDebugEnabled()) {
        log.debug("缓存对象(NAME={})不存在，直接从数据库中获取...", cache_name);
      }
      return resolver.resolve();
    }
    ValueWrapper val = cache.get(cache_key);
    T ret = null;
    if (val == null) {
      if (log.isDebugEnabled()) {
        log.debug("缓存对象(NAME={})中KEY={}不存在，从数据库中获取...", cache_name, cache_key);
      }
      cache.put(cache_key, ret = resolver.resolve());
    } else {
      ret = (T)val.get();
      if (log.isDebugEnabled()) {
        log.debug("缓存对象(NAME={})中KEY={}已存在，缓存内容：\n{}", cache_name, cache_key, ret);
      }
    }
    return ret;
  }

  private void evitAllCache(String cache_name) {
    Cache cache = cacheManager.getCache(cache_name);
    if (cache == null) return;
    cache.clear();
    if (log.isDebugEnabled()) {
      log.debug("缓存(NAME={})已全部清理", cache_name);
    }
  }

  private void evitRequestTableCache(ManipulationRequest insert_request) {
    this.clearTableCaches(insert_request.getFlat_table_name());
    this.clearServiceCachesByTable(insert_request.getFlat_table_name());
  }

  @Override
  public JdbcQueryResponse<List<Object>> getSchemas() throws SQLException {
    JdbcQueryResponse<List<Object>> ret = this.resolveCached(
      String.format("%s:meta", appInfoResolver.getAppId()),
      Constants.SCHEMAS_CACHE,
      () -> super.getSchemas()
    );
    // 校验授权
    ret.setLic(this.getAppInfoResolver().getLicenseMeta());
    return ret;
  }

  @Override
  public JdbcQueryResponse<List<Object>> getCatalogs() throws SQLException {
    JdbcQueryResponse<List<Object>> ret = this.resolveCached(
      String.format("%s:meta", appInfoResolver.getAppId()),
      Constants.CATALOGS_CACHE,
      () -> super.getCatalogs()
    );
    // 校验授权
    ret.setLic(this.getAppInfoResolver().getLicenseMeta());
    return ret;
  }

  @Override
  public JdbcQueryResponse<Page<Object>> getTables(
    ResultDefinition return_meta, String catalog, String schema_pattern,
    String table_name_pattern, Long offset, Long limit, String... types
  ) throws SQLException {
    StringBuffer cbuf = new StringBuffer(Constants.CATALOGS_CACHE);
    cbuf.append(":").append(catalog).append(":")
        .append(schema_pattern).append(":").append(table_name_pattern)
        .append(":").append(offset).append(":").append(limit);
    if (types != null && types.length > 0) {
      for (String type_name : types) {
        cbuf.append(":").append(type_name);
      }
    }
    JdbcQueryResponse<Page<Object>> ret = this.resolveCached(
      String.format("%s:meta", appInfoResolver.getAppId()),
      cbuf.toString(),
      () -> super.getTables(return_meta, catalog, schema_pattern,
        table_name_pattern, offset, limit, types)
    );
    // 校验授权
    ret.setLic(this.getAppInfoResolver().getLicenseMeta());
    return ret;
  }

  @Override
  public JdbcMetaData getMetaData() throws SQLException {
    return super.getMetaData();
  }

  @Override
  protected TableMeta doFetchResultSetMeta(JdbcQueryRequest table_query, SQLDialectProvider sql_dialect_provider) {
    if (table_query.hasTable_meta()) return table_query.getTable_meta();
    try {
      return this.resolveCached(
        String.format("%s:meta", appInfoResolver.getAppId()),
        String.format("%s:%s", Constants.TABLEMETA_CACHE, table_query.getFlat_table_name()),
        () -> super.doFetchResultSetMeta(table_query, sql_dialect_provider)
      );
    } catch (SQLException e) {
      throw new IllegalStateException(e.getMessage(), e);
    }
  }

  // 查询缓存ID
  private String buildJdbcQueryRequestCacheKey(JdbcQueryRequest table_query, String prefix) {
    StringBuffer cbuf = new StringBuffer(prefix);
    StringBuffer dbuf = new StringBuffer(table_query.getSelect().toString());
    if (table_query.hasJoin()) {
      table_query.getJoin().forEach(j -> {
        dbuf.append(j.toString());
      });
    }
    if (table_query.hasWhere()) {
      table_query.getWhere().forEach(w -> {
        dbuf.append(w.toString());
      });
    }
    if (table_query.hasGroup_by()) {
      table_query.getGroup_by().forEach(g -> {
        dbuf.append(g.toString());
      });
    }
    if (table_query.hasOrder_by()) {
      table_query.getOrder_by().forEach(o -> {
        dbuf.append(o.toString());
      });
    }
    dbuf.append(table_query.getResult().toString());
    if (log.isDebugEnabled()) {
      log.debug("查询请求内容:\n{}", table_query.toString());
    }

    if (table_query.hasDefinition() &&
      table_query.getDefinition().hasOwner_id_column() && 
      this.getLoggedUserContext().isUserLogged()
    ) {
      dbuf.append(this.getLoggedUserContext().getLoggedUser().getUserid());
    }

    if (table_query.getResult().hasColumn()) {
      table_query.getResult().getColumns().forEach((k, v) -> {
        dbuf.append(k).append(v);
      });
    }

    dbuf.append(table_query.getResult().getRow_struct())
        .append(table_query.getResult().isAll_column())
        .append(table_query.getResult().isRow_total())
        .append(table_query.getResult().isSignleton())
        .append(table_query.getResult().getOffset())
        .append(table_query.getResult().getLimit())
        .append(table_query.getResult().isColumn_compact())
        .append(table_query.getResult().isContain_meta());

    if (table_query.getResult().getEntity_class() != null) {
      dbuf.append(table_query.getResult().getEntity_class().getName());
    }

    cbuf.append(DigestUtils.sha256Hex(dbuf.toString()));
    String ret = cbuf.toString();
    if (log.isDebugEnabled()) {
      log.debug("KEY={}", ret);
    }
    return ret;
  }

  @Override
  protected JdbcQueryResponse<?> doQueryListResult(
    JdbcQueryRequest table_query,
    SQLDialectProvider sql_dialect_provider
  ) {
    try {
      JdbcQueryResponse<?> ret = this.resolveCached(
        String.format("%s:%s", appInfoResolver.getAppId(), table_query.getFlat_table_name()),
        this.buildJdbcQueryRequestCacheKey(table_query, "list:"),
        () -> super.doQueryListResult(table_query, sql_dialect_provider)
      );
      // 校验授权
      ret.setLic(getAppInfoResolver().getLicenseMeta());
      return ret;
    } catch (SQLException e) {
      throw new IllegalStateException(e.getMessage(), e);
    }
  }

  @Override
  protected Long doQueryTotalResult(JdbcQueryRequest table_query, SQLDialectProvider sql_dialect_provider) {
    try {
      return this.resolveCached(
        String.format("%s:%s", appInfoResolver.getAppId(), table_query.getFlat_table_name()),
        this.buildJdbcQueryRequestCacheKey(table_query, "total:"),
        () -> super.doQueryTotalResult(table_query, sql_dialect_provider)
      );
    } catch (SQLException e) {
      throw new IllegalStateException(e.getMessage(), e);
    }
  }

  @Override
  protected Object doInsertTableData(
    ManipulationRequest insert_request,
    SQLDialectProvider sql_dialect_provider,
    JdbcTypeConverterFactory type_converter_factory
  ) throws SQLException {
    Object ret = super.doInsertTableData(insert_request, sql_dialect_provider, type_converter_factory);
    this.evitRequestTableCache(insert_request);
    return ret;
  }

  @Override
  protected int[] doUpdateTableData(
    ManipulationRequest update_request,
    SQLDialectProvider sql_dialect_provider,
    JdbcTypeConverterFactory type_converter_factory
  ) throws SQLException {
    int[] ret = super.doUpdateTableData(update_request, sql_dialect_provider, type_converter_factory);
    this.evitRequestTableCache(update_request);
    return ret;
  }

  @Override
  protected Integer doDeleteTableData(
    ManipulationRequest delete_request,
    SQLDialectProvider sql_dialect_provider,
    JdbcTypeConverterFactory type_converter_factory
  ) throws SQLException {
    Integer ret = super.doDeleteTableData(delete_request, sql_dialect_provider, type_converter_factory);
    this.evitRequestTableCache(delete_request);
    return ret;
  }

  private String buildSQLServiceCacheKey(SQLFragment sql_fragment, Object input_data) {
    StringBuffer buf = new StringBuffer();
    buf.append(sql_fragment.getMapped_id()).append(JsonUtils.toJson(input_data));
    return DigestUtils.sha256Hex(buf.toString());
  }

  @Override
  protected <T> T doSQLServiceSelect(SqlSession session, SQLServiceRequest sql_request, SQLFragment sql_fragment, Object input_data) {
    try {
      return this.resolveCached(
        String.format("%s:%s", appInfoResolver.getAppId(), sql_request.getDefinition().getService_path()),
        this.buildSQLServiceCacheKey(sql_fragment, input_data),
        () -> super.doSQLServiceSelect(session, sql_request, sql_fragment, input_data)
      );
    } catch (SQLException e) {
      throw new IllegalStateException(e.getMessage(), e);
    }
  }

  @Override
  protected Object doSQLServiceInsert(SqlSession session, SQLServiceRequest sql_request, SQLFragment sql_fragment, Object input_data) {
    Object ret = super.doSQLServiceInsert(session, sql_request, sql_fragment, input_data);
    this.clearTableCachesByService(sql_request.getDefinition());
    return ret;
  }

  @Override
  protected Object doSQLServiceUpdate(SqlSession session, SQLServiceRequest sql_request, SQLFragment sql_fragment, Object input_data) {
    Object ret = super.doSQLServiceUpdate(session, sql_request, sql_fragment, input_data);
    this.clearTableCachesByService(sql_request.getDefinition());
    return ret;
  }

  @Override
  protected Object doSQLServiceDelete(SqlSession session, SQLServiceRequest sql_request, SQLFragment sql_fragment, Object input_data) {
    Object ret = super.doSQLServiceDelete(session, sql_request, sql_fragment, input_data);
    this.clearTableCachesByService(sql_request.getDefinition());
    return ret;
  }

  @Override
  public void clearMetaCaches() {
    this.evitAllCache(String.format("%s:meta", appInfoResolver.getAppId()));
  }


  @Override
  public void clearTableCaches(String table_name) {
    this.evitAllCache(String.format("%s:%s", appInfoResolver.getAppId(), table_name));
    this.clearServiceCachesByTable(table_name);
  }

  // 根据服务清理表缓存
  private void clearTableCachesByService(SQLServiceDefinition definition) {
    for (String table_name : definition.getUpdate_tables()) {
      this.clearTableCaches(table_name);
    }
  }

  // 根据查询表清理服务缓存
  private void clearServiceCachesByTable(String table_name) {
    serviceRegistry.getServices().forEach(s -> {
      if (s.getQuery_tables().contains(table_name)) {
        this.clearServiceCaches(s.getService_path());
      }
    });
  }

  @Override
  public void clearServiceCaches(String service_path) {
    this.evitAllCache(String.format("%s:%s", appInfoResolver.getAppId(), service_path));
  }

  @Override
  public void clearServiceCaches() {
    serviceRegistry.getServices().forEach(s -> {
      this.clearServiceCaches(s.getService_path());
    });
  }

  @Override
  @SuppressWarnings("unchecked")
  public void clearTableCaches() {
    List<Object> datalist = null;
    try {
      datalist = this.getTables(
        null, null,
         null,
        null,
        null, null
      ).getData().getData();      
    } catch (SQLException e) {
      log.warn("清理缓存发生错误: {}", e.getMessage(), e);
    }
    if (datalist == null) return;
    for (Object objdata : datalist) {
      Map<String, Object> objmap = (Map<String, Object>)objdata;
      String type_val = (String)objmap.get("TABLE_TYPE");
      if (StringUtils.containsIgnoreCase(type_val, "TABLE")) {
        String table_name = (String)objmap.get("TABLE_NAME");
        String schema_name = (String)objmap.get("TABLE_SCHEM");
        String catelog_name = (String)objmap.get("TABLE_CAT");
        if (StringUtils.isNotBlank(catelog_name) && StringUtils.isNotBlank(schema_name)) {
          this.clearTableCaches(String.format("%s.%s.%s", catelog_name, schema_name, table_name));
        } else if (StringUtils.isNotBlank(catelog_name)) {
          this.clearTableCaches(String.format("%s.%s", catelog_name, table_name));
        } else if (StringUtils.isNotBlank(schema_name)) {
          this.clearTableCaches(String.format("%s.%s", schema_name, table_name));
        }
      }
    }
  }

  @FunctionalInterface
  public static interface ValueResolver<T> {
    
    T resolve() throws SQLException;

  }

}
