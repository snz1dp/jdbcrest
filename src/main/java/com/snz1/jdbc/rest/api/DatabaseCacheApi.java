package com.snz1.jdbc.rest.api;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.snz1.jdbc.rest.service.CacheClear;
import com.snz1.jdbc.rest.service.JdbcRestProvider;

import gateway.api.Result;
import gateway.api.Return;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;

@RestController
@Tag(name = "8、数据缓存")
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
  ) {
    restProvider.clearTableCaches(table_name);
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
