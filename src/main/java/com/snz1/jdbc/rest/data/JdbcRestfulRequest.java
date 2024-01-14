package com.snz1.jdbc.rest.data;

import java.io.Serializable;
import java.util.Date;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;

import lombok.Data;

// Jdbc转Restful请求
@Data
public abstract class JdbcRestfulRequest implements Serializable, Cloneable {

  // 目录名称
  private String catalog_name;

  // 模式名称
  private String schema_name;

  // 表名
  private String table_name;

  // 请求ID
  private String request_id;

  // 定义
  private TableDefinition definition;

  // 更新时间
  private Date request_time = new Date();

  // 是否有定义
  public boolean hasDefinition() {
    return this.definition != null;
  }

  public void setDefinition(TableDefinition definition) {
    this.definition = definition;
    if (definition != null) {
      this.catalog_name = definition.getCatalog();
      this.schema_name = definition.getSchema();
      if (StringUtils.isNotBlank(definition.getAlias())) {
        this.table_name = definition.getAlias();
      }
    }
  }

  @JsonIgnore
  public String getFullTableName() {
    if (StringUtils.isBlank(this.catalog_name)) {
      if (StringUtils.isBlank(this.schema_name)) {
        return this.table_name;
      } else {
        return String.format("\"%s\".\"%s\"", this.schema_name, this.table_name);
      }
    } else if (StringUtils.isBlank(this.schema_name)) {
      return String.format("\"%s\".\"%s\"", this.catalog_name, this.table_name);
    } else {
      return String.format("\"%s\".\"%s\".\"%s\"", this.catalog_name, this.schema_name, this.table_name);
    }
  }

  public String getFlatTableName() {
    if (StringUtils.isBlank(this.catalog_name)) {
      if (StringUtils.isBlank(this.schema_name)) {
        return this.table_name;
      } else {
        return String.format("%s.%s", this.schema_name, this.table_name);
      }
    } else if (StringUtils.isBlank(this.schema_name)) {
      return String.format("%s.%s", this.catalog_name, this.table_name);
    } else {
      return String.format("%s.%s.%s", this.catalog_name, this.schema_name, this.table_name);
    }
  }

  // 克隆
  public JdbcRestfulRequest clone() {
    try {
      return (JdbcRestfulRequest)super.clone();
    } catch (CloneNotSupportedException e) {
      throw new IllegalStateException(e.getMessage(), e);
    }
  }

}
