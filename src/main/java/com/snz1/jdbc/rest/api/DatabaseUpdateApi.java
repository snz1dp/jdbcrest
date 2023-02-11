package com.snz1.jdbc.rest.api;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.springframework.web.bind.annotation.PatchMapping;
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
@Tag(name = "数据更新")
@RequestMapping
public class DatabaseUpdateApi {

  @Resource
  private JdbcRestProvider restProvider;

  @Operation(summary = "根据主键更新数据")
  @RequestMapping(path = "/tables/{table:.*}/{key:.*}", method = {
    org.springframework.web.bind.annotation.RequestMethod.POST,
    org.springframework.web.bind.annotation.RequestMethod.PUT
  })
  public Return<?> updateTableRow(
    @PathVariable("table")
    String table_name,
    @PathVariable("key")
    String key,
    HttpServletRequest request
  ) throws IOException, SQLException {
    ManipulationRequest update_request = createManipulationRequest(table_name, key, request);
    update_request.setPatch_update(false);
    Object result = restProvider.updateTableData(update_request);
    return Return.wrap(result);
  }

  @Operation(summary = "根据主键补丁更新")
  @PatchMapping(path = "/tables/{table:.*}/{key:.*}")
  public Return<?> patchTableRow(
    @PathVariable("table")
    String table_name,
    @PathVariable("key")
    String key,
    HttpServletRequest request
  ) throws IOException, SQLException {
    // 构建操作请求
    ManipulationRequest update_request = createManipulationRequest(table_name, key, request);
    update_request.setPatch_update(true);
    Object result = restProvider.updateTableData(update_request);
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

    // 构建操作请求
    ManipulationRequest update_request = new ManipulationRequest();
    update_request.setTable_name(table_name);

    // 获取表元信息
    update_request.copyTableMeta(result_meta);

    // 提取更新输入
    Object input_data = RequestUtils.fetchManipulationRequestData(request);

    // 提取自定义主键
    RequestUtils.fetchManipulationRequestCustomKey(request, update_request.getCustom_key());

    // 获取主键
    RequestCustomKey custom_key = update_request.getCustom_key();
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

    update_request.setInput_data(input_data);
    return update_request;
  }

  @Operation(summary = "条件批量更新表数据")
  @PatchMapping(path = "/tables/{table:.*}")
  public Return<?> updateTableData(
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

    // 构建操作请求
    ManipulationRequest batch_patch_request = new ManipulationRequest();
    batch_patch_request.copyTableMeta(result_meta);
    batch_patch_request.setTable_name(table_name);

    // 提取更新输入
    Object input_data = RequestUtils.fetchManipulationRequestData(request);

    // 从请求头中提取更新条件
    RequestUtils.fetchRequestHeaderWhereCondition(request, batch_patch_request.getWhere());
    Validate.isTrue(
      batch_patch_request.hasWhere(),
      "请传入条件请求头参数"
    );

    batch_patch_request.setPatch_update(true);
    batch_patch_request.setInput_data(input_data);

    Object result = restProvider.updateTableData(batch_patch_request);
    return Return.wrap(result);
  }

}
