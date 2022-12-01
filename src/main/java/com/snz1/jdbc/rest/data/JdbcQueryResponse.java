package com.snz1.jdbc.rest.data;

import java.io.Serializable;
import java.sql.JDBCType;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;

import gateway.api.Return;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper=true)
public class JdbcQueryResponse<T> extends Return<T> {

  private ResultMeta meta;

  public void setData(T d) {
    this.data = d;
  }

  public T getData() {
    return this.data;
  }

  @Data
  public static class ResultMeta implements Serializable {

    private Object primary_key;

    private List<ResultColumn> columns = new LinkedList<>();

    // 表名
    private String table_name;

    private Integer column_count;

    @JsonIgnore
    private Map<String, ResultColumn> _column_map;

    public ResultMeta addColumn(ResultColumn col) {
      this.columns.add(col);
      return this;
    }

    @JsonIgnore
    public Map<String, ResultColumn> getColumnMap() {
      if (this._column_map != null) {
        return this._column_map;
      }
      this._column_map = new HashMap<>();
      return this._column_map = this.columns.stream().collect(Collectors.toMap(
        x -> x.getName(), x -> x
      ));
    }

    public static ResultMeta of(ResultSetMetaData rs_meta, JdbcQueryRequest.ResultMeta request, Object primary_key) throws SQLException {
      ResultMeta metadata = new ResultMeta();
      metadata.setPrimary_key(primary_key);
      int col_count = rs_meta.getColumnCount();
      metadata.setColumn_count(col_count);

      for (int i = 1; i <= col_count; i++) {
        if (metadata.getTable_name() == null) {
          metadata.setTable_name(rs_meta.getTableName(i));
        }

        String column_name = rs_meta.getColumnName(i);
        if (request != null &&
          request.getColumns().size() > 0 &&
          !request.getColumns().containsKey(column_name)
        ) {
          continue;
        }

        if (request != null && request.getColumns().containsKey(column_name) && request.getColumns().get(column_name).hasAlias()) {
          column_name = request.getColumns().get(column_name).getAlias();
        }
        ResultColumn col = new ResultColumn();
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

    @Data
    public static class ResultColumn implements Serializable{
      
      // 序号
      private Integer index;

      // 字段名称
      private String name;

      // 字段标签
      private String label;

      // 类型
      private String sql_type;

      // JdbcType
      private JDBCType jdbc_type;

      // 显示宽度
      private Integer display_size;

      // 精度
      private Integer precision;

      // 刻度
      private Integer scale;

      // 是否只读
      private Boolean read_only;

      // 是否可写
      private Boolean writable;

      // 是否自动增长
      private Boolean auto_increment;

      // 是否可用于查询条件
      private Boolean searchable;

      // 是否货币类型字段
      private Boolean currency;

      // 是否可空
      private Boolean nullable;

      // 是否区分大小写
      private Boolean case_sensitive;

      // 表名
      private String table_name;

    }

  }

}
