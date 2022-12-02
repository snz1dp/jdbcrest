package com.snz1.jdbc.rest.api;

import java.sql.SQLException;
import java.util.List;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.snz1.jdbc.rest.data.JdbcQueryResponse;
import com.snz1.jdbc.rest.data.TableQueryRequest;
import com.snz1.jdbc.rest.service.JdbcRestProvider;
import com.snz1.jdbc.rest.utils.RequestUtils;

import gateway.api.NotFoundException;
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
  public Return<?> queryTable(
    @ApiParam("表名")
    @PathVariable("table")
    String table_name,
    HttpServletRequest request
  ) throws SQLException {
    TableQueryRequest table_query = new TableQueryRequest(); 
    table_query.setTable_name(table_name);
    RequestUtils.fetchJdbcQueryRequest(request, table_query);
    if (table_query.getSelect().hasCount()) {
      if (table_query.hasGroup_by()) {
        return restProvider.queryGroupCountResult(table_query);
      } else {
        return Return.wrap(restProvider.queryAllCountResult(table_query));
      }
    } else if (table_query.hasGroup_by()) { 
      return restProvider.queryGroupResult(table_query);
    } else if (table_query.getResult().isSignleton()) {
      return restProvider.querySignletonResult(table_query);
    } else {
      return restProvider.queryPageResult(table_query);
    }
  }

  @SuppressWarnings("unchecked")
  @ApiOperation("行查询")
  @GetMapping(path = "/tables/{table:.*}/{key:.*}")
  public Return<?> queryTableRow(
    @ApiParam("表名")
    @PathVariable("table")
    String table_name,
    @ApiParam("主键")
    @PathVariable("key")
    String key,
    HttpServletRequest request
  ) throws SQLException {
    TableQueryRequest table_query = new TableQueryRequest();
    int keyname_start = key.indexOf('$', 1);
    if (keyname_start > 0) {
      String key_composes[] = StringUtils.split(key, '|');
      for (String keyv : key_composes) {
        String keyval = keyv.substring(0, keyname_start);
        String keycolumn = keyv.substring(keyname_start + 1);
        TableQueryRequest.WhereCloumn where_col = TableQueryRequest.WhereCloumn.of((String)keycolumn);
        where_col.addCondition(TableQueryRequest.ConditionOperation.$eq, keyval);
        table_query.getWhere().add(where_col);
      }
    } else {
      Object keycolumn = restProvider.getTablePrimaryKey(table_name);
      if (keycolumn == null) {
        throw new NotFoundException("数据不存在");
      }
      if (keycolumn instanceof List) {
        List<Object> keycolumns = (List<Object>)keycolumn;
        String key_values[] = StringUtils.split(key, '|');
        if (keycolumns.size() == key_values.length) {
          throw new IllegalArgumentException("主键数据不正确");
        }
        for (int i = 0; i < key_values.length; i++) {
          TableQueryRequest.WhereCloumn where_col = TableQueryRequest.WhereCloumn.of((String)keycolumns.get(i));
          where_col.addCondition(TableQueryRequest.ConditionOperation.$eq, key_values[i]);
          table_query.getWhere().add(where_col);
        } 
      } else {
        TableQueryRequest.WhereCloumn where_col = TableQueryRequest.WhereCloumn.of((String)keycolumn);
        where_col.addCondition(TableQueryRequest.ConditionOperation.$eq, key);
        table_query.getWhere().add(where_col);
      }
    }
    table_query.setTable_name(table_name);
    RequestUtils.fetchQueryRequestResultMeta(request, table_query.getResult());
    table_query.getResult().setSignleton(true);
    JdbcQueryResponse<?> ret = restProvider.querySignletonResult(table_query);
    if (ret.getData() == null) {
      throw new NotFoundException("数据不存在");
    }
    return ret;
  }

  @ApiOperation("表元信息")
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
    RequestUtils.fetchQueryRequestResultMeta(request, table_query.getResult());
    return Return.wrap(restProvider.queryResultMeta(table_query));
  }

}
