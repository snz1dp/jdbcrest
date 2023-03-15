package com.snz1.jdbc.rest.api;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.snz1.jdbc.rest.data.ManipulationRequest;
import com.snz1.jdbc.rest.data.RequestCustomKey;
import com.snz1.jdbc.rest.data.TableMeta;
import com.snz1.jdbc.rest.data.JdbcQueryRequest;
import com.snz1.jdbc.rest.service.JdbcRestProvider;
import com.snz1.jdbc.rest.utils.RequestUtils;

import gateway.api.NotFoundException;
import gateway.api.Return;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;

@RestController
@Tag(name = "数据删除")
@RequestMapping
public class DatabaseDeleteApi {

  @Resource
  private JdbcRestProvider restProvider;

  @Operation(summary = "根据主键删除数据")
  @DeleteMapping(path = "/tables/{table:.*}/{key:.*}")
  public Return<?> deleteTableRow(
    @PathVariable("table")
    String table_name,
    @PathVariable("key")
    String key,
    HttpServletRequest request
  ) throws IOException, SQLException {
    ManipulationRequest delete_request = createManipulationRequest(table_name, key, request);
    delete_request.setPatch_update(RequestUtils.testRequestUpdateIsPatchMode(request));
    Object result = restProvider.deleteTableData(delete_request);
    return Return.wrap(result);
  }

  @SuppressWarnings("unchecked")
  private ManipulationRequest createManipulationRequest(
    String table_name, String key,
    HttpServletRequest request
  ) throws IOException, SQLException {

    // 获取表元信息
    TableMeta result_meta = restProvider.queryResultMeta(JdbcQueryRequest.of(table_name));
    if (StringUtils.contains(table_name, ".")) {
      table_name = String.format("%s.%s", result_meta.getSchemas_name(), result_meta.getTable_name());
    } else {
      table_name = result_meta.getTable_name();
    }

    if (result_meta.hasDefinition()) {
      Validate.isTrue(
        !result_meta.getDefinition().isReadonly()
        && result_meta.getDefinition().isPublish(),
        "不允许对%s进行操作", table_name
      );
    }

    // 构建操作请求
    ManipulationRequest delete_request = new ManipulationRequest();
    delete_request.setTable_name(table_name);

    // 提取自定义主键
    RequestUtils.fetchManipulationRequestCustomKey(request, delete_request.getCustom_key());

    // 获取表元信息
    delete_request.copyTableMeta(result_meta);

    // 获取主键
    RequestCustomKey custom_key = delete_request.getCustom_key();
    Object keycolumn = custom_key.hasCustom_key() ? custom_key.getCustom_key() : delete_request.getRow_key();
    if (keycolumn == null) {
      throw new NotFoundException("主键不存在");
    }

    if (keycolumn instanceof List) { // 主键为列表表示为复合组件
      List<Object> keycolumns = (List<Object>)keycolumn;
      String key_values[] = StringUtils.split(key, custom_key.getKey_splitter());
      if (keycolumns.size() != key_values.length) {
        throw new IllegalArgumentException("主键不正确");
      }
      delete_request.setInput_key(Arrays.asList(key_values));
    } else {
      delete_request.setInput_key(key);
    }
    return delete_request;
  }

  @Operation(summary = "批量删除表数据")
  @DeleteMapping(path = "/tables/{table:.*}")
  public Return<?> deleteTableData(
    @PathVariable("table")
    String table_name,
    HttpServletRequest request
  ) throws IOException, SQLException {

    // 获取表元信息
    TableMeta result_meta = restProvider.queryResultMeta(JdbcQueryRequest.of(table_name));
    if (StringUtils.contains(table_name, ".")) {
      table_name = String.format("%s.%s", result_meta.getSchemas_name(), result_meta.getTable_name());
    } else {
      table_name = result_meta.getTable_name();
    }

    if (result_meta.hasDefinition()) {
      Validate.isTrue(
        !result_meta.getDefinition().isReadonly()
        && result_meta.getDefinition().isPublish(),
        "不允许对%s进行操作", table_name
      );
    }

    // 构建操作请求
    ManipulationRequest delete_request = new ManipulationRequest();
    delete_request.setTable_name(table_name);

    // 获取表元信息
    delete_request.copyTableMeta(result_meta);

    // 提取删除条件
    RequestUtils.fetchQueryRequestWhereCondition(request, delete_request.getWhere());
    Validate.isTrue(
      delete_request.hasWhere(),
      "请传入删除条件参数"
    );
    Object result = restProvider.deleteTableData(delete_request);
    return Return.wrap(result);
  }

}
