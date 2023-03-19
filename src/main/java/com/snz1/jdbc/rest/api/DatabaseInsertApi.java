package com.snz1.jdbc.rest.api;

import java.io.IOException;
import java.sql.SQLException;
import java.util.UUID;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.Validate;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.snz1.jdbc.rest.data.ManipulationRequest;
import com.snz1.jdbc.rest.data.TableMeta;
import com.snz1.jdbc.rest.data.JdbcQueryRequest;
import com.snz1.jdbc.rest.service.JdbcRestProvider;
import com.snz1.jdbc.rest.utils.RequestUtils;

import gateway.api.Return;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;

@RestController
@Tag(name = "数据插入")
@RequestMapping
public class DatabaseInsertApi {

  @Resource
  private JdbcRestProvider restProvider;

  @Operation(summary = "插入数据表")
  @RequestMapping(path = "/tables/{table:.*}", method = {
    org.springframework.web.bind.annotation.RequestMethod.POST,
    org.springframework.web.bind.annotation.RequestMethod.PUT
  })
  public Return<?> createTableData(
    @PathVariable("table")
    String table_name,
    HttpServletRequest request
  ) throws SQLException, IOException {
    TableMeta result_meta = restProvider.queryResultMeta(JdbcQueryRequest.of(table_name, request));

    if (result_meta.hasDefinition()) {
      Validate.isTrue(
        !result_meta.getDefinition().isReadonly()
        && result_meta.getDefinition().isPublish(),
        "不允许对%s进行操作", table_name
      );
    }

    ManipulationRequest insert_request = new ManipulationRequest();
    // 获取表元信息
    insert_request.copyTableMeta(result_meta);
    // 提取插入输入
    insert_request.setInput_data(RequestUtils.fetchManipulationRequestData(request));

    Object result = restProvider.insertTableData(insert_request);
    return Return.wrap(result);
  }

  @Operation(summary = "获取对象UUID")
  @RequestMapping(path = "/uuid", method = {
    org.springframework.web.bind.annotation.RequestMethod.POST,
    org.springframework.web.bind.annotation.RequestMethod.GET
  })
  public Return<String> createObjectId() {
    return Return.wrap(UUID.randomUUID().toString());
  }

}
