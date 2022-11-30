package com.snz1.jdbc.rest.api;

import java.sql.SQLException;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.snz1.jdbc.rest.data.JdbcQueryResponse;
import com.snz1.jdbc.rest.data.TableQueryRequest;
import com.snz1.jdbc.rest.service.JdbcRestProvider;
import com.snz1.jdbc.rest.utils.RequestUtils;

import gateway.api.Return;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@RestController
@Api(tags = "2、数据查询")
@RequestMapping
public class DatabaseQueryApi {

  @Resource
  private JdbcRestProvider restProvider;

  @ApiOperation("表查询")
  @RequestMapping(path = "/tables/{table:.*}", method = {
    RequestMethod.GET,
    RequestMethod.POST,
  })
  @ResponseBody
  public JdbcQueryResponse<?> queryTable(
    @ApiParam("表名")
    @PathVariable("table")
    String table_name,
    HttpServletRequest request
  ) throws SQLException {
    TableQueryRequest table_query = new TableQueryRequest(); 
    table_query.setTable_name(table_name);
    RequestUtils.fetchRequestQueryMetaData(request, table_query.getResult_meta());
    return restProvider.queryPageResult(table_query);
  }

  @ApiOperation("元信息")
  @RequestMapping(path = "/tables/{table:.*}/meta", method = {
    RequestMethod.GET,
    RequestMethod.POST,
  })
  @ResponseBody
  public Return<JdbcQueryResponse.ResultMeta> queryMeta(
    @ApiParam("表名")
    @PathVariable("table")
    String table_name,
    HttpServletRequest request
  ) throws SQLException {
    TableQueryRequest table_query = new TableQueryRequest(); 
    table_query.setTable_name(table_name);
    RequestUtils.fetchRequestQueryMetaData(request, table_query.getResult_meta());
    return Return.wrap(restProvider.queryResultMeta(table_query));
  }

}
