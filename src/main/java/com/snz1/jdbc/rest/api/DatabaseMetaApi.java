package com.snz1.jdbc.rest.api;

import java.sql.SQLException;
import java.util.List;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.snz1.jdbc.rest.data.JdbcMetaData;
import com.snz1.jdbc.rest.data.ResultDefinition;
import com.snz1.jdbc.rest.data.TableMeta;
import com.snz1.jdbc.rest.data.TableQueryRequest;
import com.snz1.jdbc.rest.service.JdbcRestProvider;
import com.snz1.jdbc.rest.utils.RequestUtils;

import gateway.api.Return;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@RestController
@Api(tags = "1、数据信息")
@RequestMapping
public class DatabaseMetaApi {
  
  @Resource
  private JdbcRestProvider restProvider;

  @ApiOperation("服务元信息")
  @GetMapping(path = "/meta")
  public Return<JdbcMetaData> getMetaData() throws SQLException {
    return Return.wrap(restProvider.getMetaData());
  }

  @ApiOperation("获取模式")
  @GetMapping(path = "/schemas")
  public Return<List<Object>> getSchemas() throws SQLException {
    return restProvider.getSchemas();
  }

  @ApiOperation("获取目录")
  @GetMapping(path = "/catalogs")
  public Return<List<Object>> getCatalogs() throws SQLException {
    return restProvider.getCatalogs();
  }

  @ApiOperation("获取数据表")
  @GetMapping(path = "/tables")
  public Return<List<Object>> getTables(
    @ApiParam("目录")
    @RequestParam(value="catalog", required = false)
    String catalog,
    @ApiParam("模式")
    @RequestParam(value="schema", required = false)
    String schemaPattern,
    @ApiParam("表名")
    @RequestParam(value="table", required = false)
    String tableNamePattern,
    @ApiParam("类型")
    @RequestParam(value="type", required = false)
    String []types,
    HttpServletRequest request
  ) throws SQLException {
    ResultDefinition result_meta = new ResultDefinition();
    RequestUtils.fetchQueryRequestResultMeta(request, result_meta);
    return restProvider.getTables(result_meta, catalog, schemaPattern, tableNamePattern, types);
  }

  @ApiOperation("表元信息")
  @RequestMapping(path = "/tables/{table:.*}/meta", method = {
    RequestMethod.GET,
    RequestMethod.POST,
  })
  public Return<TableMeta> queryMeta(
    @ApiParam("表名")
    @PathVariable("table")
    String table_name,
    HttpServletRequest request
  ) throws SQLException {
    TableQueryRequest table_query = new TableQueryRequest(); 
    table_query.setTable_name(table_name);
    RequestUtils.fetchQueryRequestResultMeta(request, table_query.getResult());
    return Return.wrap(restProvider.queryResultMeta(table_query));
  }

}
