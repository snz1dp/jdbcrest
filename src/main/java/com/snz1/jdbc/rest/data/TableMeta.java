package com.snz1.jdbc.rest.data;

import java.io.Serializable;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ToString
@EqualsAndHashCode
public class TableMeta implements Serializable {

  // 主键
  @Setter
  @Getter
  private Object primary_key;

  // 字段
  @Getter
  @Setter
  private List<TableColumn> columns = new LinkedList<>();

  // 表名
  @Getter
  @Setter
  private String table_name;

  @Getter
  @Setter
  private String catalog_name;

  // Schemas
  @Getter
  @Setter
  private String schema_name;

  // 字段统计
  @Getter
  @Setter
  private Integer column_count;

  @Getter
  @Setter
  private List<TableIndex> normal_indexs;

  // 唯一索引
  @JsonIgnore
  private TableIndex unique_index;

  @JsonIgnore
  private List<TableIndex> _unique_index;

  // 定义
  @Getter
  @Setter
  private TableDefinition definition;
  
  public boolean hasDefinition() {
    return this.definition != null;
  }

  public TableMeta addColumn(TableColumn col) {
    this.columns.add(col);
    return this;
  }

  public boolean hasColumns() {
    return this.columns != null && this.columns.size() > 0;
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

  // 是否有唯一索引
  public boolean hasUnique_index() {
    return this.unique_index != null;
  }

  // 是否有唯一主键
  public boolean hasPrimary_key() {
    return this.primary_key != null;
  }

  // 是否有行主键，包括唯一索引
  public boolean hasRow_key() {
    return this.hasPrimary_key() || this.hasUnique_index();
  }

  // 获取行主键
  public Object getRow_key() {
    if (this.hasPrimary_key()) return this.getPrimary_key();
    if (this.getUnique_index() == null) return null;
    if (this.getUnique_indexs().size() == 1) return this.getUnique_index().getColumn();
    List<String> keynames = new LinkedList<>();
    for (TableIndex keyindex : this.getUnique_indexs()) {
      keynames.add(keyindex.getName());
    }
    return keynames;
  }

  public boolean testRow_key(String column) {
    Object row_key = getRow_key();
    if (row_key == null) return false;
    if (row_key instanceof List) {
      List<?> row_keylist = (List<?>)row_key;
      for (int i = 0; i < row_keylist.size(); i++) {
        if (StringUtils.equalsIgnoreCase(column, (String)row_keylist.get(i))) {
          return true;
        }
      }
      return false;
    } else {
      return StringUtils.equalsIgnoreCase(column, (String)row_key);
    }
  }

  public void setUnique_indexs(List<TableIndex> unique_index) {
    if (unique_index == null || unique_index.size() == 0) {
      this.unique_index = null;
      this._unique_index = null;
    } else if (unique_index.size() > 0) {
      this.unique_index = unique_index.get(0);
      this._unique_index = unique_index;
    }
  }

  public List<TableIndex> getUnique_indexs() {
    return _unique_index;
  }

  public void setUnique_index(TableIndex unique_index) {
    if (unique_index == null) {
      this.unique_index = null;
      this._unique_index = null;
    } else {
      this.unique_index = unique_index;
      this._unique_index = new ArrayList<>(Arrays.asList(unique_index));
    }
  }

  @JsonIgnore
  public TableIndex getUnique_index() {
    return unique_index;
  }

  public TableColumn findColumn(String column) {
    int table_end = column.indexOf('.');
    if (table_end != -1) {
      String table_name = column.substring(0, table_end);
      column = column.substring(table_end + 1);
      for (TableColumn col : columns) {
        if (StringUtils.equalsIgnoreCase(column, col.getName()) && 
          StringUtils.equalsIgnoreCase(table_name, col.getTable_name())
        ) {
          return col;
        }
      }
    } else {
      for (TableColumn col : columns) {
        if (StringUtils.equalsIgnoreCase(column, col.getName())) {
          return col;
        }
      }
    }
    return null;
  }

  public static TableMeta of(
    ResultSetMetaData rs_meta,
    ResultDefinition request,
    Object primary_key,
    TableIndexs table_index,
    TableDefinition definition
  ) throws SQLException {
    if (rs_meta == null) return null;
    TableMeta metadata = new TableMeta();
    metadata.setPrimary_key(primary_key);
    int col_count = rs_meta.getColumnCount();
    metadata.setColumn_count(col_count);
    metadata.setDefinition(definition);
    if (table_index != null) {
      metadata.setUnique_indexs(table_index.getUnique_indexs());
      metadata.setNormal_indexs(table_index.getNormal_indexs());
    }

    for (int i = 1; i <= col_count; i++) {
      if (metadata.table_name == null) {
        try {
          metadata.setTable_name(rs_meta.getTableName(i));
        } catch(SQLException e) {}
      }

      if (metadata.getSchema_name() == null) {
        try {
          metadata.setSchema_name(rs_meta.getSchemaName(i));
        } catch(SQLException e) {}
      }

      if (metadata.getCatalog_name() == null) {
        try {
          metadata.setCatalog_name(rs_meta.getCatalogName(i));
        } catch(SQLException e) {}
      }

      String column_name = rs_meta.getColumnName(i);
      if (request != null &&
        !request.isAll_column() &&
        request.getColumns().size() > 0 &&
        !request.getColumns().containsKey(column_name)
      ) {
        continue;
      }

      if (request != null && request.getColumns().containsKey(column_name) && request.getColumns().get(column_name).hasAlias()) {
        column_name = request.getColumns().get(column_name).getAlias();
      }
      TableColumn col = new TableColumn();
      col.setIndex(i - 1);
      col.setName(column_name);
      col.setLabel(rs_meta.getColumnLabel(i));
      col.setSql_type(rs_meta.getColumnTypeName(i));
      try {
        col.setJdbc_type(JDBCType.valueOf(rs_meta.getColumnType(i)));
      } catch(IllegalArgumentException e) {
        log.warn("数据类型(DATA_TYPE={})无法转成JDBC类型", rs_meta.getColumnType(i));
      }
      col.setAuto_increment(rs_meta.isAutoIncrement(i));
      col.setNullable(rs_meta.isNullable(i) == ResultSetMetaData.columnNullable);
      col.setScale(rs_meta.getScale(i));
      col.setPrecision(rs_meta.getPrecision(i));
      col.setColumn_size(rs_meta.getPrecision(i));
      col.setDisplay_size(rs_meta.getColumnDisplaySize(i));
      try {
        if (StringUtils.isNotBlank(rs_meta.getTableName(i))) {
          col.setTable_name(rs_meta.getTableName(i));
        }
      } catch(SQLException e) {}
      metadata.getColumns().add(col);
    }

    if (StringUtils.isBlank(metadata.table_name)) {
      metadata.setTable_name(null);
    }

    if (StringUtils.isBlank(metadata.getSchema_name())) {
      metadata.setSchema_name(null);
    }

    if (StringUtils.isBlank(metadata.getCatalog_name())) {
      metadata.setCatalog_name(null);
    }

    return metadata;
  }


  public static TableMeta of(
    ResultSet rs_meta,
    ResultDefinition request,
    Object primary_key,
    TableIndexs table_index,
    TableDefinition definition
  ) throws SQLException {
    TableMeta metadata = new TableMeta();
    metadata.setPrimary_key(primary_key);
    int col_count = 0;

    while (rs_meta != null && rs_meta.next()) {
      if (metadata.getCatalog_name() == null) {
        metadata.setCatalog_name(rs_meta.getString("TABLE_CAT"));
      }
      if (metadata.getSchema_name() == null) {
        metadata.setSchema_name(rs_meta.getString("TABLE_SCHEM"));
      }
      if (metadata.getTable_name() == null) {
        metadata.setTable_name(rs_meta.getString("TABLE_NAME"));
      }

      String column_name = rs_meta.getString("COLUMN_NAME");
      if (request != null &&
        !request.isAll_column() &&
        request.getColumns().size() > 0 &&
        !request.getColumns().containsKey(column_name)
      ) {
        continue;
      }

      if (request != null && request.getColumns().containsKey(column_name) && request.getColumns().get(column_name).hasAlias()) {
        column_name = request.getColumns().get(column_name).getAlias();
      }

      TableColumn col = new TableColumn();
      col.setIndex(col_count);
      col.setName(column_name);
      col.setLabel(rs_meta.getString("REMARKS"));
      if (StringUtils.isBlank(col.getLabel())) {
        col.setLabel(col.getName());
      }

      col.setSql_type(rs_meta.getString("TYPE_NAME"));
      try {
        col.setJdbc_type(JDBCType.valueOf(rs_meta.getInt("DATA_TYPE")));
      } catch(IllegalArgumentException e) {
        log.warn("数据类型(DATA_TYPE={})无法转成JDBC类型", rs_meta.getInt("DATA_TYPE"));
      }
 
      col.setAuto_increment(StringUtils.equals("YES", rs_meta.getString("IS_AUTOINCREMENT")));
      col.setNullable(StringUtils.equals("YES", rs_meta.getString("IS_NULLABLE")));
      col.setScale(rs_meta.getInt("NUM_PREC_RADIX"));
      col.setPrecision(rs_meta.getInt("DECIMAL_DIGITS"));
      col.setColumn_size(rs_meta.getInt("COLUMN_SIZE"));
      col.setDisplay_size(rs_meta.getInt("CHAR_OCTET_LENGTH"));

      String table_name = rs_meta.getString("TABLE_NAME");
      if (StringUtils.isNotBlank(table_name)) {
        col.setTable_name(table_name);
      }

      metadata.getColumns().add(col);
      col_count++;
    }

    metadata.setColumn_count(col_count);
    metadata.setDefinition(definition);

    if (table_index != null) {
      metadata.setUnique_indexs(table_index.getUnique_indexs());
      metadata.setNormal_indexs(table_index.getNormal_indexs());
    }

    if (StringUtils.isBlank(metadata.getCatalog_name())) {
      metadata.setCatalog_name(null);
    }
    if (StringUtils.isBlank(metadata.getSchema_name())) {
      metadata.setSchema_name(null);
    }
    if (StringUtils.isBlank(metadata.getTable_name())) {
      metadata.setTable_name(null);
    }

    return metadata;
  }

}