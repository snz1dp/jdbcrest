package com.snz1.jdbc.rest.data;

import java.io.Serializable;
import java.sql.JDBCType;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import lombok.Data;

@Data
public class TableMeta implements Serializable {

  private Object primary_key;

  private List<TableColumn> columns = new LinkedList<>();

  // 表名
  private String table_name;

  private Integer column_count;

  public TableMeta addColumn(TableColumn col) {
    this.columns.add(col);
    return this;
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

  public static TableMeta of(ResultSetMetaData rs_meta, JdbcQueryRequest.ResultMeta request, Object primary_key) throws SQLException {
    TableMeta metadata = new TableMeta();
    metadata.setPrimary_key(primary_key);
    int col_count = rs_meta.getColumnCount();
    metadata.setColumn_count(col_count);

    for (int i = 1; i <= col_count; i++) {
      if (metadata.getTable_name() == null) {
        metadata.setTable_name(rs_meta.getTableName(i));
      }

      String column_name = rs_meta.getColumnName(i);
      if (request != null &&
        !request.isAll_columns() &&
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

    return metadata;
  }

}