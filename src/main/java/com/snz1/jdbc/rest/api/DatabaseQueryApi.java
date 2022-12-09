package com.snz1.jdbc.rest.api;

import java.sql.SQLException;
import java.util.List;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.snz1.jdbc.rest.data.JdbcQueryResponse;
import com.snz1.jdbc.rest.data.RequestCustomKey;
import com.snz1.jdbc.rest.data.TableColumn;
import com.snz1.jdbc.rest.data.TableMeta;
import com.snz1.jdbc.rest.data.JdbcQueryRequest;
import com.snz1.jdbc.rest.data.WhereCloumn;
import com.snz1.jdbc.rest.service.JdbcRestProvider;
import com.snz1.jdbc.rest.service.TableDefinitionRegistry;
import com.snz1.jdbc.rest.utils.RequestUtils;

import gateway.api.NotFoundException;
import gateway.api.Return;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;

@RestController
@Tag(name = "2、数据查询")
@RequestMapping
public class DatabaseQueryApi {

  @Resource
  private JdbcRestProvider restProvider;

  @Resource
  private TableDefinitionRegistry definitionRegistry;

  @Operation(summary = "分页查询")
  @GetMapping(path = "/tables/{table:.*}")
  @ResponseBody
  public Return<?> getTablePage(
    @PathVariable("table")
    String table_name,
    HttpServletRequest request
  ) throws SQLException {
    return doQueryTable(table_name, request);
  }

  @Operation(summary = "高级查询")
  @PostMapping(path = "/query")
  @ResponseBody
  public Return<?> queryTable(
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
    TableMeta result_meta = restProvider.queryResultMeta(JdbcQueryRequest.of(table_name));
    table_name = result_meta.getTable_name();

    JdbcQueryRequest table_query = new JdbcQueryRequest(); 
    table_query.setTable_name(table_name);
    table_query.copyTableMeta(result_meta);

    RequestUtils.fetchJdbcQueryRequest(request, table_query);

    if (table_query.hasWhere()) {
      JdbcQueryRequest metaquery = table_query.clone();
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

  @SuppressWarnings("unchecked")
  @Operation(summary = "主键查询")
  @GetMapping(path = "/tables/{table:.*}/{key:.*}")
  public Return<?> queryTableRow(
    @PathVariable("table")
    String table_name,
    @PathVariable("key")
    String key,
    HttpServletRequest request
  ) throws SQLException {
    TableMeta table_meta = restProvider.queryResultMeta(JdbcQueryRequest.of(table_name));
    table_name = table_meta.getTable_name();

    // 获取表元信息
    JdbcQueryRequest table_query = new JdbcQueryRequest();
    table_query.setTable_name(table_name);
    table_query.copyTableMeta(table_meta);

    // 获取自定义主键
    RequestCustomKey custom_key = RequestUtils.fetchManipulationRequestCustomKey(request, new RequestCustomKey());

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
        WhereCloumn where_col = WhereCloumn.of((String)keycolumns.get(i));
        TableColumn col = table_query.getTable_meta().findColumn(where_col.getColumn());
        if (col != null) {
          where_col.setType(col.getJdbc_type());
        }
        where_col.addCondition(JdbcQueryRequest.ConditionOperation.$eq, key_values[i]);
        table_query.getWhere().add(where_col);
      } 
    } else {
      WhereCloumn where_col = WhereCloumn.of((String)keycolumn);
      TableColumn col = table_query.getTable_meta().findColumn(where_col.getColumn());
      if (col != null) {
        where_col.setType(col.getJdbc_type());
      }
      where_col.addCondition(JdbcQueryRequest.ConditionOperation.$eq, key);
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

}
