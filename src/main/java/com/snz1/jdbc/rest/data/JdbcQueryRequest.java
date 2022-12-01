package com.snz1.jdbc.rest.data;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.sql.JDBCType;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.snz1.jdbc.rest.utils.JdbcUtils;

import lombok.Data;

@Data
public class JdbcQueryRequest implements Serializable {

  // 请求ID
  private String request_id;

  // 查询请求
  private SelectMeta select = new SelectMeta();

  // 分组
  private List<GroupBy> group_by = new LinkedList<>();

  // 排序
  private List<OrderBy> order_by = new LinkedList<>();

  // 条件过滤
  private List<WhereCloumn> where = new LinkedList<>();

  // 查询结果描述
  private ResultMeta result = new ResultMeta();

  // 有分组
  public boolean hasGroup_by() {
    return this.group_by != null && this.group_by.size() > 0;
  }

  // 有排序
  public boolean hasOrder_by() {
    return this.order_by != null && this.order_by.size() > 0;
  }

  // 有查询条件
  public boolean hasWhere() {
    return this.where != null && this.where.size() > 0;
  }

  //排序
  @Data
  public static class OrderBy implements Serializable {

    private String column;

    private Sort sort;

    public static OrderBy of(String column) {
      OrderBy orderby = new OrderBy();
      orderby.setColumn(column);
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

    public String toHavingSQL() {
      if (this.operation.parameter_count() == 0) {
        return String.format("%s(%s) %s", this.func, this.column, this.operation.operator());
      } else if (this.operation.parameter_count() == 1) {
        return String.format("%s(%s) %s ?", this.func, this.column, this.operation.operator());
      } else if (this.operation.parameter_count() == 2) {
        return String.format("%s(%s) %s ? and ?", this.func, this.column, this.operation.operator());
      } else {
        this.setArray(JdbcUtils.toArray(this.getValue(), this.type));
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

    public void buildParameters(List<Object> parameters) {
      if (this.operation.parameter_count() == 1) {
        parameters.add(Array.get(JdbcUtils.toArray(this.value, this.type), 0));
      } else if (this.operation.parameter_count() == 2) {
        Object data = JdbcUtils.toArray(this.value, this.type);
        parameters.add(Array.get(data, 0));
        parameters.add(Array.get(data, 1));
      } else if (this.operation.parameter_count() == 3) {
        for (int i = 0; i < this.getArray_length(); i++) {
          parameters.add(Array.get(this.getArray(), i));
        }
      }
    }

  }

  public static enum ConditionOperation {

    $eq("=", 1),
    $gt(">", 1),
    $gte(">=", 1),
    $lt("<", 1),
    $lte("<=", 1),
    $ne("<>", 1),
    $in("in", 3),
    $nin("not in", 3),
    $isnull("is null", 0),
    $notnull("is not null", 0),
    $istrue("is true", 0),
    $nottrue("is not true", 0),
    $isfalse("is false", 0),
    $notfalse("is not false", 0),
    $like("like", 1),
    $ilike("ilike", 1),
    $nlike("not like", 1),
    $nilike("not ilike", 1),
    $between("between", 2),

    ;

    private String operator;

    private int parameter_count = 1;

    private ConditionOperation(String operator, int parameter_count) {
      this.operator = operator;
      this.parameter_count = parameter_count;
    }

    public String operator() {
      return this.operator;
    }

    public int parameter_count() {
      return this.parameter_count;
    }

  }
  
  @Data
  public static class SelectMeta implements Serializable {

    // 是否去除重复
    private boolean distinct;

    // 统计
    private String count;

    // 查询字段
    private List<SelectColumn> columns = new LinkedList<>();

    // 是否有总计
    public boolean hasCount() {
      return StringUtils.isNotBlank(this.count);
    }

    public boolean hasColumns() {
      return this.columns.size() > 0;
    }

    // 列
    @Data
    public static class SelectColumn implements Serializable {

      // 字段名
      private String column;

      // 函数
      private String function;

      // 定义AS
      private String as;

      public static SelectColumn of(String name) {
        SelectColumn ret = new SelectColumn();
        ret.setColumn(name);
        return ret;
      }

      public boolean hasAs() {
        return as != null;
      }

      public boolean hasFunction() {
        return function != null;
      }

    }

  }

  @Data
  public static class WhereCloumn implements Serializable {

    private String column;

    private Condition condition;

    private JDBCType type;

    private List<Condition> conditions;

    public static WhereCloumn of(String name) {
      WhereCloumn where_column = new WhereCloumn();
      where_column.setColumn(name);
      return where_column;
    }

    public void addCondition(ConditionOperation operation, String val) {
      Condition temp_condition = new Condition();
      temp_condition.setOperation(operation);
      temp_condition.setValue(val);
      if (this.condition == null && this.conditions == null) {
        this.condition = temp_condition;
      } else {
        if (this.conditions == null) {
          this.conditions = new LinkedList<>();
          this.conditions.add(this.condition);
        }
        this.condition = null;
        this.conditions.add(temp_condition);
      }
    }

    public String toWhereSQL() {
      if (this.condition != null) {
        ConditionOperation operation = this.condition.getOperation();
        if (operation.parameter_count() == 0) {
          return String.format("%s %s", this.column, operation.operator());
        } else if (operation.parameter_count() == 1) {
          return String.format("%s %s ?", this.column, operation.operator());
        } else if (operation.parameter_count() == 2) {
          return String.format("%s %s ? and ?", this.column, operation.operator());
        } else {
          condition.setArray(JdbcUtils.toArray(condition.getValue(), this.type));
          StringBuffer parambuf = new StringBuffer();
          boolean paramappend = false;
          for (int i = 0; i < condition.getArray_length(); i++) {
            if (paramappend) {
              parambuf.append(", ");
            } else {
              paramappend = true;
            }
            parambuf.append("?");
          }
          return String.format("%s %s (%s)", this.column, operation.operator(), parambuf.toString());
        }
      } else {
        StringBuffer sqlbuf = new StringBuffer("(");
        boolean where_append = false;
        for (Condition condition : this.conditions) {
          if (where_append) {
            sqlbuf.append(" and ");
          } else {
            where_append = true;
          }
          ConditionOperation operation = condition.getOperation();
          if (operation.parameter_count() == 0) {
            sqlbuf.append(String.format("%s %s", this.column, operation.operator()));
          } else if (operation.parameter_count() == 1) {
            sqlbuf.append(String.format("%s %s ?", this.column, operation.operator()));
          } else if (operation.parameter_count() == 2) {
            sqlbuf.append(String.format("%s %s ? and ?", this.column, operation.operator()));
          } else {
            condition.setArray(JdbcUtils.toArray(condition.getValue(), this.type));
            StringBuffer parambuf = new StringBuffer();
            boolean paramappend = false;
            for (int i = 0; i < condition.getArray_length(); i++) {
              if (paramappend) {
                parambuf.append(", ");
              } else {
                paramappend = true;
              }
              parambuf.append("?");
            }
            sqlbuf.append(String.format("%s %s (%s)", this.column, operation.operator(), parambuf.toString()));
          }
        }
        sqlbuf.append(")");
        return sqlbuf.toString();
      }
    }

    public void buildParameters(List<Object> parameters) {
      if (this.condition != null) {
        ConditionOperation operation = condition.getOperation();
        if (operation.parameter_count() == 1) {
          parameters.add(this.condition.getValue());
        } else if (operation.parameter_count() == 2) {
          Object data = JdbcUtils.toArray(condition.getValue(), this.type);
          parameters.add(Array.get(data, 0));
          parameters.add(Array.get(data, 0));
        } else if (operation.parameter_count() == 3) {
          for (int i = 0; i < condition.getArray_length(); i++) {
            parameters.add(Array.get(condition.getArray(), i));
          }
        }
      } else {
        for (Condition condition : this.conditions) {
          ConditionOperation operation = condition.getOperation();
          if (operation.parameter_count() == 1) {
            parameters.add(this.condition.getValue());
          } else if (operation.parameter_count() == 2) {
            Object data = JdbcUtils.toArray(condition.getValue(), this.type);
            parameters.add(Array.get(data, 0));
            parameters.add(Array.get(data, 0));
          } else if (operation.parameter_count() == 3) {
            for (int i = 0; i < condition.getArray_length(); i++) {
              parameters.add(Array.get(condition.getArray(), i));
            }
          }
        }
      }
    }

    @Data
    public static class Condition implements Serializable {

      private ConditionOperation operation;

      private String value;
  
      private Object array;

      public int getArray_length() {
        if (array == null) return 0;
        return Array.getLength(array);
      }

    }

  }


  // 返回描述
  @Data
  public static class ResultMeta implements Serializable {

    // 返回所有字段
    @Deprecated
    private boolean all_columns = true;

    // 单对象返回
    private boolean signleton = false;

    // 字段
    private Map<String, ResultColumn> columns = new HashMap<String, ResultColumn>();

    // 是否包含元信息
    private boolean contain_meta = false;

    // 对象结构
    private ResultObjectStruct row_struct = ResultObjectStruct.map;

    // 仅一列则返回紧凑模式
    private boolean column_compact;

    // 开始索引
    private long offset;

    // 返回限制
    private long limit;

    // 列
    @Data
    public static class ResultColumn implements Serializable {

      // 字段名
      private String name;

      // 别名
      private String alias;

      private ColumnType type = ColumnType.raw;

      public static ResultColumn of(String name) {
        ResultColumn ret = new ResultColumn();
        ret.setName(name);
        return ret;
      }

      public boolean hasAlias() {
        return alias != null;
      }

    }

    // 列类型
    public static enum ColumnType {

      raw,

      map,

      list,

      base64;

    }

    // 返回的对象数据结构
    public static enum ResultObjectStruct {

      // 返回对象
      map("对象"),
      
      // 返回列表
      list("列表");

      private String title;

      private ResultObjectStruct(String title) {
        this.title = title;
      }

      public String title() {
        return title;
      }

    }

  }

}
