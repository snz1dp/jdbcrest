package com.snz1.jdbc.rest.data;

import java.io.Serializable;
import java.sql.JDBCType;
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

  // Schemas
  @Getter
  @Setter
  private String schemas_name;

  // 字段统计
  @Getter
  @Setter
  private Integer column_count;

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
    List<TableIndex> unique_index,
    TableDefinition definition
  ) throws SQLException {
    TableMeta metadata = new TableMeta();
    metadata.setPrimary_key(primary_key);
    int col_count = rs_meta.getColumnCount();
    metadata.setColumn_count(col_count);
    metadata.setUnique_indexs(unique_index);
    metadata.setDefinition(definition);

    for (int i = 1; i <= col_count; i++) {
      if (metadata.getTable_name() == null) {
        metadata.setTable_name(rs_meta.getTableName(i));
      }

      if (metadata.getSchemas_name() == null) {
        metadata.setSchemas_name(rs_meta.getSchemaName(i));
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
      col.setCurrency(rs_meta.isCurrency(i));
      col.setSearchable(rs_meta.isSearchable(i));
      col.setSql_type(rs_meta.getColumnTypeName(i));
      col.setJdbc_type(JDBCType.valueOf(rs_meta.getColumnType(i)));
      col.setAuto_increment(rs_meta.isAutoIncrement(i));
      col.setNullable(rs_meta.isNullable(i) == ResultSetMetaData.columnNullable);
      col.setRead_only(rs_meta.isReadOnly(i));
      col.setWritable(rs_meta.isWritable(i));
      col.setScale(rs_meta.getScale(i));
      col.setPrecision(rs_meta.getPrecision(i));
      col.setDisplay_size(rs_meta.getColumnDisplaySize(i));
      col.setCase_sensitive(rs_meta.isCaseSensitive(i));
      if (StringUtils.isNotBlank(rs_meta.getTableName(i))) {
        col.setTable_name(rs_meta.getTableName(i));
      }
      metadata.getColumns().add(col);
    }

    if (StringUtils.isBlank(metadata.getTable_name())) {
      metadata.setTable_name(null);
    }

    if (StringUtils.isBlank(metadata.getSchemas_name())) {
      metadata.setSchemas_name(null);
    }

    return metadata;
  }

}