package com.snz1.jdbc.rest.api;

import java.io.IOException;
import java.sql.SQLException;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.Validate;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.snz1.jdbc.rest.data.JdbcDMLRequest;
import com.snz1.jdbc.rest.data.ManipulationRequest;
import com.snz1.jdbc.rest.data.TableDefinition;
import com.snz1.jdbc.rest.service.AppInfoResolver;
import com.snz1.jdbc.rest.service.JdbcRestProvider;
import com.snz1.jdbc.rest.service.TableDefinitionRegistry;

import gateway.api.Return;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;

@RestController("jdbcrest::DatabaseDMLApi")
@Tag(name = "高级功能")
@RequestMapping
@ConditionalOnProperty(name = "app.jdbcrest.api.enabled", havingValue = "true")
public class DatabaseDMLApi {

  @Resource
  private JdbcRestProvider restProvider;

  @Resource
  private AppInfoResolver appInfoResolver;

  @Resource
  private TableDefinitionRegistry tableDefinitionRegistry;

  @Operation(summary = "批量DML")
  @PostMapping(path = "/dml")
  public Return<?> updateData(
    @RequestBody
    JdbcDMLRequest []dmls,
    HttpServletRequest request
  ) throws SQLException, IOException {
    if (appInfoResolver.isGlobalReadonly()) {
      throw new IllegalStateException("不允许的操作");
    }
    for (int i = 0; i < dmls.length; i++) {
      JdbcDMLRequest dml = dmls[0];
      if (dml.getInsert() != null && dml.getInsert().length > 0) {
        for (int j = 0; j < dml.getInsert().length; j++) {
          ManipulationRequest insert_request = dml.getInsert()[j];
          String table_name = insert_request.getFullTableName();
          Validate.notBlank(table_name, "[%d-%d]插入请求未设置表名", i, j);
          if (appInfoResolver.isStrictMode()) {
            Validate.isTrue(
              tableDefinitionRegistry.hasTableDefinition(table_name),
              "%s不存在", table_name);
          } 
          if (tableDefinitionRegistry.hasTableDefinition(table_name)) {
            TableDefinition tdf  = tableDefinitionRegistry.getTableDefinition(table_name);
            Validate.isTrue(
              !tdf.testReadonly() && tdf.testPublish(),
              "不允许对%s进行操作", table_name);
          }
        }
      }
      if (dml.getUpdate() != null && dml.getUpdate().length > 0) {
        for (int j = 0; j < dml.getUpdate().length; j++) {
          ManipulationRequest update_request = dml.getUpdate()[j];
          String table_name = update_request.getFullTableName();
          Validate.notBlank(table_name, "[%d-%d]更新请求未设置表名", i, j);
          Validate.isTrue(update_request.hasWhere(), "[%d-%d]更新请求未设置Where条件", i, j);
          if (appInfoResolver.isStrictMode()) {
            Validate.isTrue(
              tableDefinitionRegistry.hasTableDefinition(table_name),
              "%s不存在", table_name);
          } 
          if (tableDefinitionRegistry.hasTableDefinition(table_name)) {
            TableDefinition tdf  = tableDefinitionRegistry.getTableDefinition(table_name);
            Validate.isTrue(
              !tdf.testReadonly() && tdf.testPublish(),
              "不允许对%s进行操作", table_name);
          }
        }
      }
      if (dml.getDelete() != null && dml.getDelete().length > 0) {
        for (int j = 0; j < dml.getDelete().length; j++) {
          ManipulationRequest delete_request = dml.getDelete()[j];
          String table_name = delete_request.getFullTableName();
          Validate.notBlank(table_name, "[%d-%d]删除请求未设置表名", i, j);
          Validate.isTrue(delete_request.hasWhere(), "[%d-%d]删除请求未设置Where条件", i, j);
          if (appInfoResolver.isStrictMode()) {
            Validate.isTrue(
              tableDefinitionRegistry.hasTableDefinition(table_name),
              "%s不存在", table_name);
          } 
          if (tableDefinitionRegistry.hasTableDefinition(table_name)) {
            TableDefinition tdf  = tableDefinitionRegistry.getTableDefinition(table_name);
            Validate.isTrue(
              !tdf.testReadonly() && tdf.testPublish(),
              "不允许对%s进行操作", table_name);
          }
        }
      }
    }

    return Return.wrap(restProvider.executeDMLRequest(dmls));
  }

}
