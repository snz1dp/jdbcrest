package com.snz1.jdbc.rest.api;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.snz1.jdbc.rest.data.ManipulationRequest;
import com.snz1.jdbc.rest.data.JdbcQueryResponse;
import com.snz1.jdbc.rest.data.RequestCustomKey;
import com.snz1.jdbc.rest.data.TableColumn;
import com.snz1.jdbc.rest.data.TableMeta;
import com.snz1.jdbc.rest.data.TableQueryRequest;
import com.snz1.jdbc.rest.service.JdbcRestProvider;
import com.snz1.jdbc.rest.utils.RequestUtils;

import gateway.api.NotFoundException;
import gateway.api.Return;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@RestController
@Api(tags = "2、数据服务")
@RequestMapping
public class DatabaseQueryApi {

  @Resource
  private JdbcRestProvider restProvider;

  @ApiOperation("分页查询")
  @GetMapping(path = "/tables/{table:.*}")
  @ResponseBody
  public Return<?> getTablePage(
    @ApiParam("表名")
    @PathVariable("table")
    String table_name,
    HttpServletRequest request
  ) throws SQLException {
    return doQueryTable(table_name, request);
  }

  @ApiOperation("高级查询")
  @PostMapping(path = "/query")
  @ResponseBody
  public Return<?> queryTable(
    @ApiParam("表名")
    @RequestParam("table")
    String table_name,
    HttpServletRequest request
  ) throws SQLException {
    return doQueryTable(table_name, request);
  }

  private Return<?> doQueryTable(
    String table_name,
    HttpServletRequest request
  ) throws SQLException {
    TableQueryRequest table_query = new TableQueryRequest(); 
    table_query.setTable_name(table_name);
    RequestUtils.fetchJdbcQueryRequest(request, table_query);

    if (table_query.hasWhere()) {
      TableQueryRequest metaquery = table_query.clone();
      metaquery.resetWhere();
      metaquery.resetGroup_by();
      table_query.setTable_meta(restProvider.queryResultMeta(metaquery));
      table_query.rebuildWhere();
    }

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


  @ApiOperation("数据创建")
  @PostMapping(path = "/tables/{table:.*}")
  @ResponseBody
  public Return<?> createData(
    @ApiParam("表名")
    @PathVariable("table")
    String table_name,
    HttpServletRequest request
  ) throws SQLException, IOException {
    if (!restProvider.testTableExisted(table_name)) {
      throw new IllegalStateException("数据表不存在");
    }
    TableMeta result_meta = restProvider.queryResultMeta(TableQueryRequest.of(table_name));
    ManipulationRequest insert_request = new ManipulationRequest();
    insert_request.copyTableMeta(result_meta);
    insert_request.setInput_data(RequestUtils.fetchManipulationRequestData(request));
    Object result = restProvider.insertTableData(insert_request);
    return Return.wrap(result);
  }

  @SuppressWarnings("unchecked")
  @ApiOperation("主键更新")
  @RequestMapping(path = "/tables/{table:.*}/{key:.*}")
  public Return<?> updateTableRow(
    @ApiParam("表名")
    @PathVariable("table")
    String table_name,
    @ApiParam("主键")
    @PathVariable("key")
    String key,
    HttpServletRequest request
  ) throws IOException, SQLException {
    // 获取表元信息
    TableMeta result_meta = restProvider.queryResultMeta(TableQueryRequest.of(table_name));
    ManipulationRequest update_request = new ManipulationRequest();
    update_request.setTable_name(table_name);
    // 提取自定义主键
    RequestUtils.fetchManipulationRequestCustomKey(request, update_request.getCustom_key());
    update_request.copyTableMeta(result_meta);

    RequestCustomKey custom_key = update_request.getCustom_key();
    // 获取主键
    Object keycolumn = custom_key.hasCustom_key() ? custom_key.getCustom_key() : update_request.getRow_key();
    if (keycolumn == null) {
      throw new NotFoundException("主键不存在");
    }

    if (keycolumn instanceof List) { // 主键为列表表示为复合组件
      List<Object> keycolumns = (List<Object>)keycolumn;
      String key_values[] = StringUtils.split(key, custom_key.getKey_splitter());
      if (keycolumns.size() != key_values.length) {
        throw new IllegalArgumentException("主键不正确");
      }
      update_request.setInput_key(Arrays.asList(key_values));
    } else {
      update_request.setInput_key(key);
    }

    // 提取更新输入
    update_request.setInput_data(RequestUtils.fetchManipulationRequestData(request));

    Object result = restProvider.updateTableData(update_request);
    return Return.wrap(result);
  }

  @SuppressWarnings("unchecked")
  @ApiOperation("主键查询")
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
    // 获取自定义主键
    RequestCustomKey custom_key = RequestUtils.fetchManipulationRequestCustomKey(request, new RequestCustomKey());

    // 获取表元信息
    TableQueryRequest table_query = new TableQueryRequest();
    table_query.setTable_name(table_name);
    table_query.setTable_meta(restProvider.queryResultMeta(TableQueryRequest.of(table_name)));

    // 获取主键
    Object keycolumn = custom_key.hasCustom_key() ? custom_key.getCustom_key() : table_query.getTable_meta().getRow_key();
    if (keycolumn == null) {
      throw new NotFoundException("主键不存在");
    }

    if (keycolumn instanceof List) { // 主键为列表表示为复合组件
      List<Object> keycolumns = (List<Object>)keycolumn;
      String key_values[] = StringUtils.split(key, custom_key.getKey_splitter());
      if (keycolumns.size() != key_values.length) {
        throw new IllegalArgumentException("主键不正确");
      }
      for (int i = 0; i < key_values.length; i++) {
        TableQueryRequest.WhereCloumn where_col = TableQueryRequest.WhereCloumn.of((String)keycolumns.get(i));
        TableColumn col = table_query.getTable_meta().findColumn(where_col.getColumn());
        if (col != null) {
          where_col.setType(col.getJdbc_type());
        }
        where_col.addCondition(TableQueryRequest.ConditionOperation.$eq, key_values[i]);
        table_query.getWhere().add(where_col);
      } 
    } else {
      TableQueryRequest.WhereCloumn where_col = TableQueryRequest.WhereCloumn.of((String)keycolumn);
      TableColumn col = table_query.getTable_meta().findColumn(where_col.getColumn());
      if (col != null) {
        where_col.setType(col.getJdbc_type());
      }
      where_col.addCondition(TableQueryRequest.ConditionOperation.$eq, key);
      table_query.getWhere().add(where_col);
    }

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
