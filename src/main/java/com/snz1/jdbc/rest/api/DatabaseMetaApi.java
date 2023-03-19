package com.snz1.jdbc.rest.api;

import java.sql.SQLException;
import java.util.List;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.Validate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.snz1.jdbc.rest.data.JdbcMetaData;
import com.snz1.jdbc.rest.data.ResultDefinition;
import com.snz1.jdbc.rest.data.TableMeta;
import com.snz1.jdbc.rest.data.JdbcQueryRequest;
import com.snz1.jdbc.rest.data.Page;
import com.snz1.jdbc.rest.service.AppInfoResolver;
import com.snz1.jdbc.rest.service.JdbcRestProvider;
import com.snz1.jdbc.rest.service.TableDefinitionRegistry;
import com.snz1.jdbc.rest.utils.RequestUtils;

import gateway.api.Return;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;

@RestController
@Tag(name = "数据信息")
@RequestMapping
public class DatabaseMetaApi {
  
  @Resource
  private JdbcRestProvider restProvider;

  @Resource
  private AppInfoResolver appInfoResolver;

  @Resource
  private TableDefinitionRegistry tableRegistry;

  @Operation(summary = "服务元信息")
  @GetMapping(path = "/meta")
  public Return<JdbcMetaData> getMetaData() throws SQLException {
    return Return.wrap(restProvider.getMetaData());
  }

  @Operation(summary = "获取模式")
  @GetMapping(path = "/schemas")
  public Return<List<Object>> getSchemas() throws SQLException {
    if (appInfoResolver.isStrictMode()) {
      return Return.success();
    }
    return restProvider.getSchemas();
  }

  @Operation(summary = "获取目录")
  @GetMapping(path = "/catalogs")
  public Return<List<Object>> getCatalogs() throws SQLException {
    if (appInfoResolver.isStrictMode()) {
      return Return.success();
    }
    return restProvider.getCatalogs();
  }

  @Operation(summary = "获取数据表")
  @GetMapping(path = "/tables")
  public Return<Page<Object>> getTables(
    @RequestParam(value="catalog", required = false)
    String catalog,
    @RequestParam(value="schema", required = false)
    String schemaPattern,
    @RequestParam(value="table", required = false)
    String tableNamePattern,
    @RequestParam(value="type", required = false)
    String []types,
    @RequestParam(value = "offset", defaultValue = "0")
    Long offset,
    @RequestParam(value = "limit", defaultValue = "0")
    Long limit,
    HttpServletRequest request
  ) throws SQLException {
    if (appInfoResolver.isStrictMode()) {
      return Return.success();
    }
    ResultDefinition result_meta = new ResultDefinition();
    RequestUtils.fetchQueryRequestResultMeta(request, result_meta);
    return restProvider.getTables(
      result_meta, catalog, schemaPattern,
      tableNamePattern, offset, limit, types);
  }

  @Operation(summary = "表元信息")
  @RequestMapping(path = "/tables/{table:.*}/meta", method = {
    RequestMethod.GET,
    RequestMethod.POST,
  })
  public Return<TableMeta> queryMeta(
    @PathVariable("table")
    String table_name,
    HttpServletRequest request
  ) throws SQLException {
    TableMeta result_meta = restProvider.queryResultMeta(JdbcQueryRequest.of(table_name, request));

    if (result_meta.hasDefinition()) {
      Validate.isTrue(
        result_meta.getDefinition().isPublish(),
        "%s不存在", table_name
      );
    }

    JdbcQueryRequest table_query = new JdbcQueryRequest(); 
    table_query.copyTableMeta(result_meta);
    RequestUtils.fetchQueryRequestResultMeta(request, table_query.getResult());
    return Return.wrap(restProvider.queryResultMeta(table_query));
  }

}
