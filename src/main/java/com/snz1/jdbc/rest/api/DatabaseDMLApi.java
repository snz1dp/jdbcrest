package com.snz1.jdbc.rest.api;

import java.io.IOException;
import java.sql.SQLException;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.snz1.jdbc.rest.data.JdbcDMLRequest;
import com.snz1.jdbc.rest.service.JdbcRestProvider;

import gateway.api.Return;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;

@RestController
@Tag(name = "6、高级功能")
@RequestMapping
public class DatabaseDMLApi {

  @Resource
  private JdbcRestProvider restProvider;

  @Operation(summary = "批量DML")
  @PostMapping(path = "/dml")
  public Return<?> updateData(
    @RequestBody
    JdbcDMLRequest []dmls,
    HttpServletRequest request
  ) throws SQLException, IOException {
    return Return.wrap(restProvider.executeDMLRequest(dmls));
  }

}
