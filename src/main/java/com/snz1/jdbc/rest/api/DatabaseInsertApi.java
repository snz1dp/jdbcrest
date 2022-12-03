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
import com.snz1.jdbc.rest.data.TableQueryRequest;
import com.snz1.jdbc.rest.service.JdbcRestProvider;
import com.snz1.jdbc.rest.utils.RequestUtils;

import gateway.api.Return;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@RestController
@Api(tags = "3、数据插入")
@RequestMapping
public class DatabaseInsertApi {

  @Resource
  private JdbcRestProvider restProvider;

  @ApiOperation("插入数据表")
  @RequestMapping(path = "/tables/{table:.*}", method = {
    org.springframework.web.bind.annotation.RequestMethod.POST,
    org.springframework.web.bind.annotation.RequestMethod.PUT
  })
  public Return<?> createTableData(
    @ApiParam("表名")
    @PathVariable("table")
    String table_name,
    HttpServletRequest request
  ) throws SQLException, IOException {
    TableMeta result_meta = restProvider.queryResultMeta(TableQueryRequest.of(table_name));
    ManipulationRequest insert_request = new ManipulationRequest();
    insert_request.copyTableMeta(result_meta);
    insert_request.setInput_data(RequestUtils.fetchManipulationRequestData(request));
    Object result = restProvider.insertTableData(insert_request);
    return Return.wrap(result);
  }

  @ApiOperation("多表数据插入")
  @RequestMapping(path = "/insert", method = {
    org.springframework.web.bind.annotation.RequestMethod.POST,
    org.springframework.web.bind.annotation.RequestMethod.PUT
  })
  public Return<?> createData(
    HttpServletRequest request
  ) throws SQLException, IOException {
    throw new IllegalStateException("未实现");
  }

}
