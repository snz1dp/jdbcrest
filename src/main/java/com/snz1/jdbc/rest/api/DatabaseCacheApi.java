package com.snz1.jdbc.rest.api;

import java.sql.SQLException;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.Validate;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.snz1.jdbc.rest.data.JdbcQueryRequest;
import com.snz1.jdbc.rest.data.TableMeta;
import com.snz1.jdbc.rest.service.CacheClear;
import com.snz1.jdbc.rest.service.JdbcRestProvider;

import gateway.api.Result;
import gateway.api.Return;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;

@RestController("jdbcrest::DatabaseCacheApi")
@Tag(name = "数据缓存")
@RequestMapping
public class DatabaseCacheApi {

  @Resource
  private JdbcRestProvider restProvider;

  @Resource
  private CacheClear cacheClear;

  @Operation(summary = "数据表查询缓存清理")
  @RequestMapping(path = "/cache/tables/{table:.*}", method = {
    org.springframework.web.bind.annotation.RequestMethod.DELETE
  })
  public Result deleteTableCache(
    @PathVariable("table")
    String table_name,
    HttpServletRequest request
  ) throws SQLException {
    TableMeta result_meta = restProvider.queryResultMeta(JdbcQueryRequest.of(table_name, request));

    if (result_meta.hasDefinition()) {
      Validate.isTrue(
        result_meta.getDefinition().testPublish(),
        "%s不存在", result_meta.getFullTableName()
      );
    }
    restProvider.clearTableCaches(result_meta.getTable_name());
    restProvider.clearTableCaches(result_meta.getFullTableName());
    return Return.success();
  }

  @Operation(summary = "所有表查询缓存清理")
  @RequestMapping(path = "/cache/tables", method = {
    org.springframework.web.bind.annotation.RequestMethod.DELETE
  })
  public Result deleteTableCache(
    HttpServletRequest request
  ) {
    restProvider.clearTableCaches();
    return Return.success();
  }

  @Operation(summary = "数据元信息缓存清理")
  @RequestMapping(path = "/cache/meta", method = {
    org.springframework.web.bind.annotation.RequestMethod.DELETE
  })
  public Result deleteMetaCache(
    HttpServletRequest request
  ) {
    restProvider.clearMetaCaches();
    return Return.success();
  }

  @Operation(summary = "所有缓存信息清理")
  @RequestMapping(path = "/cache", method = {
    org.springframework.web.bind.annotation.RequestMethod.DELETE
  })
  public Result deleteAllCache(
    HttpServletRequest request
  ) {
    cacheClear.clearCaches();
    return Return.success();
  }

}

