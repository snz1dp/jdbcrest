package com.snz1.jdbc.rest.data;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.sql.JDBCType;
import java.util.LinkedList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.snz1.jdbc.rest.Constants;
import com.snz1.jdbc.rest.service.JdbcTypeConverterFactory;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class JdbcQueryRequest extends JdbcRestfulRequest {

  // 查询请求
  private SelectMeta select = new SelectMeta();

  // 关联
  private List<Join> join = new LinkedList<>();

  // 分组
  private List<GroupBy> group_by = new LinkedList<>();

  // 排序
  private List<OrderBy> order_by = new LinkedList<>();

  // 条件过滤
  private List<WhereCloumn> where = new LinkedList<>();

  // 查询结果描述
  private ResultDefinition result = new ResultDefinition();

  // 表元信息
  private TableMeta table_meta;

  // 克隆
  public JdbcQueryRequest clone() {
    return (JdbcQueryRequest)super.clone();
  }

  public void resetWhere() {
    this.where = new LinkedList<>();
  }

  public void resetGroup_by() {
    this.order_by = new LinkedList<>();
  }

  // 有表元信息
  public boolean hasTable_meta() {
    return this.table_meta != null;
  }

  // 重新编译查询条件
  public void rebuildWhere() {
    if (!this.hasTable_meta()) return;
    this.where.forEach(w -> {
      if (w.getType() != null) return;
      TableColumn col = getTable_meta().findColumn(w.getColumn());
      if (col == null) return;
      w.setType(col.getJdbc_type());
    });
  }

  // 有分组
  public boolean hasGroup_by() {
    return this.group_by != null && this.group_by.size() > 0;
  }

  // 有关联
  public boolean hasJoin() {
    return this.join != null && this.join.size() > 0;
  }

  // 有排序
  public boolean hasOrder_by() {
    return this.order_by != null && this.order_by.size() > 0;
  }

  // 有查询条件
  public boolean hasWhere() {
    return this.where != null && this.where.size() > 0;
  }

  // 复制表元信息
  public void copyTableMeta(TableMeta table_meta) {
    this.setTable_meta(table_meta);
    this.setDefinition(table_meta.getDefinition());
    this.setCatalog_name(table_meta.getCatalog_name());
    this.setSchema_name(table_meta.getSchema_name());
    this.setTable_name(table_meta.getTable_name());
  }

  //排序
  @Data
  public static class OrderBy implements Serializable {

    private String column;

    private Sort sort;

    public static OrderBy of(String column) {
      String order_cols[] = StringUtils.split(column, " ", 2);
      OrderBy orderby = new OrderBy();
      orderby.setColumn(order_cols[0]);
      if (order_cols.length > 1) {
        orderby.setSort(Sort.valueOf(order_cols[1]));
      }
      return orderby;
    }

    public String toOrderSQL() {
      StringBuffer orderbuf = new StringBuffer();
      orderbuf.append(column);
      if (sort != null) {
        orderbuf.append(" ").append(sort);
      }
      return orderbuf.toString();
    }

    public static enum Sort {
      asc,
      desc
      ;
    }

  }

  @Data
  public static class GroupBy implements Serializable {

    private String column;

    private Having having;

    public static GroupBy of(String name) {
      GroupBy groupby = new GroupBy();
      groupby.setColumn(name);
      return groupby;
    }

    public boolean hasHaving() {
      return this.having != null;
    }

  }

  // 分组条件
  @Data
  public static class Having implements Serializable {

    // 字段名
    private String column;

    private JDBCType type;

    // 函数
    private String func;

    private ConditionOperation operation;

    private String value;

    private Object array;

    public int getArray_length() {
      if (array == null) return 0;
      return Array.getLength(array);
    }

    public static Having of(String func, String name, ConditionOperation condition, String value) {
      Having ret = new Having();
      ret.setColumn(name);
      ret.setFunc(func);
      ret.setOperation(condition);
      ret.setValue(value);
      return ret;
    }

    public String toHavingSQL(JdbcTypeConverterFactory factory) {
      if (this.operation.parameter_count() == 0) {
        return String.format("%s(%s) %s", this.func, this.column, this.operation.operator());
      } else if (this.operation.parameter_count() == 1) {
        return String.format("%s(%s) %s ?", this.func, this.column, this.operation.operator());
      } else if (this.operation.parameter_count() == 2) {
        return String.format("%s(%s) %s ? and ?", this.func, this.column, this.operation.operator());
      } else {
        this.setArray(factory.convertArray(this.getValue(), this.type));
        StringBuffer parambuf = new StringBuffer();
        boolean paramappend = false;
        for (int i = 0; i < this.getArray_length(); i++) {
          if (paramappend) {
            parambuf.append(", ");
          } else {
            paramappend = true;
          }
          parambuf.append("?");
        }
        return String.format("%s %s (%s)", this.column, operation.operator(), parambuf.toString());
      }
    }

    public void buildParameters(List<Object> parameters, JdbcTypeConverterFactory factory) {
      if (this.operation.parameter_count() == 1) {
        parameters.add(factory.convertObject(this.value, this.type));
      } else if (this.operation.parameter_count() == 2) {
        Object data = factory.convertArray(this.value, this.type);
        parameters.add(Array.get(data, 0));
        parameters.add(Array.get(data, 1));
      } else if (this.operation.parameter_count() == 3) {
        for (int i = 0; i < this.getArray_length(); i++) {
          parameters.add(Array.get(this.getArray(), i));
        }
      }
    }

  }

  @Data
  public static class Join implements Serializable {

    private String catalog_name;

    private String schema_name;

    private String table_name;

    private String join_column;

    private String outer_column;

    private String join_type;

    @JsonIgnore
    public String getFull_table_name() {
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

    public static Join of(String join_type, String catalog_name, String schema_name, String table_name, String join_column, String right_column) {
      Join join = new Join();
      join.setJoin_type(join_type);
      if (StringUtils.isNotBlank(catalog_name)) {
        join.setCatalog_name(catalog_name);
      }
      if (StringUtils.isNotBlank(schema_name)) {
        join.setSchema_name(schema_name);
      }
      join.setTable_name(table_name);
      join.setJoin_column(join_column);
      join.setOuter_column(right_column);
      return join;
    }

  }

  public static JdbcQueryRequest of(String catalog_name, String schema_name, String table_name) {
    JdbcQueryRequest treq = new JdbcQueryRequest();
    if (StringUtils.isNotBlank(catalog_name)) {
      treq.setCatalog_name(catalog_name);
    }
    if (StringUtils.isNotBlank(schema_name)) {
      treq.setSchema_name(schema_name);
    }
    treq.setTable_name(table_name);
    return treq;
  }

  public static JdbcQueryRequest of(String table_name) {
    return of(table_name, null);
  }

  public static JdbcQueryRequest of(String table_name, HttpServletRequest request) {
    JdbcQueryRequest treq = new JdbcQueryRequest();
    treq.setTable_name(table_name);
    if (request == null) return treq;
    String catalog_name = request.getParameter(Constants.CATALOG_ARG);
    if (StringUtils.isNotBlank(catalog_name)) {
      treq.setCatalog_name(catalog_name);
    }
    String schema_name = request.getParameter(Constants.SCHEMA_ARG);
    if (StringUtils.isNotBlank(schema_name)) {
      treq.setSchema_name(schema_name);
    }
    return treq;
  }

}
