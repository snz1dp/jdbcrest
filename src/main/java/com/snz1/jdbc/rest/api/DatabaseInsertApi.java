package com.snz1.jdbc.rest.api;

import java.io.IOException;
import java.sql.SQLException;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;

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
@Tag(name = "3、数据插入")
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
    TableMeta result_meta = restProvider.queryResultMeta(JdbcQueryRequest.of(table_name));
    table_name = result_meta.getTable_name();

    ManipulationRequest insert_request = new ManipulationRequest();
    insert_request.copyTableMeta(result_meta);
    insert_request.setInput_data(RequestUtils.fetchManipulationRequestData(request));

    Object result = restProvider.insertTableData(insert_request);
    return Return.wrap(result);
  }

}
