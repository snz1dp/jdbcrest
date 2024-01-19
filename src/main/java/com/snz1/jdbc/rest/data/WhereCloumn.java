package com.snz1.jdbc.rest.data;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.sql.JDBCType;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.snz1.jdbc.rest.provider.SQLDialectProvider;

import lombok.Data;

@Data
public class WhereCloumn implements Serializable, Cloneable {

  private String column;

  private WhereCloumn.Condition condition;

  private JDBCType type;

  private Boolean or;

  private List<WhereCloumn.Condition> conditions;

  private List<WhereCloumn> childrens;

  public WhereCloumn addChild(WhereCloumn where) {
    if (this.childrens == null) {
      this.childrens = new LinkedList<>();
    }
    this.childrens.add(where);
    return this;
  }

  public WhereCloumn Or() {
    this.setOr(true);
    return this;
  }

  public WhereCloumn And() {
    this.setOr(false);
    return this;
  }

  public static WhereCloumn of(String name) {
    WhereCloumn where_column = new WhereCloumn();
    where_column.setColumn(name);
    return where_column;
  }

  public static WhereCloumn of(String name, ConditionOperation operation, Object val) {
    WhereCloumn where_column = new WhereCloumn();
    where_column.setColumn(name);
    where_column.addCondition(operation, val);
    return where_column;
  }

  public static WhereCloumn of(String name, ConditionOperation operation, Object val, JDBCType type) {
    WhereCloumn where_column = new WhereCloumn();
    where_column.setColumn(name);
    where_column.addCondition(operation, val);
    where_column.setType(type);
    return where_column;
  }

  public static WhereCloumn of(String name, ConditionOperation operation) {
    WhereCloumn where_column = new WhereCloumn();
    where_column.setColumn(name);
    where_column.addCondition(operation, null);
    return where_column;
  }

  @Override
  public WhereCloumn clone() {
    try {
      final WhereCloumn w = (WhereCloumn)super.clone();
      w.condition = null;
      w.conditions = null;
      if (this.condition != null) {
        w.addCondition(this.condition.getOperation(), this.condition.value);
      } else if (this.conditions != null && this.conditions.size() > 0) {
        this.conditions.forEach(c -> {
          w.addCondition(c.getOperation(), c.value);
        });
      }
      return w;
    } catch (CloneNotSupportedException e) {
      throw new IllegalStateException(e.getMessage(), e);
    }
  }

  public WhereCloumn.Condition addCondition(ConditionOperation operation, Object val) {
    WhereCloumn.Condition temp_condition = new Condition();
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
    return temp_condition;
  }

  public WhereCloumn.Condition addCondition(String column, ConditionOperation operation, Object val) {
    WhereCloumn.Condition temp_condition = new Condition();
    temp_condition.setOperation(operation);
    temp_condition.setValue(val);
    temp_condition.setColumn(column);
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
    return temp_condition;
  }

  private boolean testOr() {
    return this.getOr() != null && this.getOr();
  }

  private static void buildOneParams(SQLDialectProvider provider, String column, WhereCloumn.Condition condition, ConditionOperation operation, JDBCType type, StringBuffer sqlbuf, List<Object> parameters) {
    sqlbuf.append("\"").append(column).append("\" ").append(operation.operator()).append(" ");
    if (condition.getValue() instanceof JdbcQueryRequest) {
      JdbcQueryStatement sts = provider.prepareQuerySelect((JdbcQueryRequest)condition.getValue());
      sqlbuf.append("(");
      sqlbuf.append(sts.getSql());
      sqlbuf.append(")");
      if (sts.hasParameter()) {
        parameters.addAll(sts.getParameters());
      }
    } else {
      sqlbuf.append("?");
      parameters.add(provider.getTypeConverterFactory().convertObject(condition.getValue(), type));
    }
  }

  @SuppressWarnings("unchecked")
  private static void buildTwoParams(SQLDialectProvider provider, String column, WhereCloumn.Condition condition, ConditionOperation operation, JDBCType type, StringBuffer sqlbuf, List<Object> parameters) {
    sqlbuf.append("\"").append(column).append("\" ").append(operation.operator()).append(" ");
    Object input = condition.getValue();
    if (input instanceof Iterable) {
      AtomicBoolean and_append = new AtomicBoolean(false);
      ((Iterable<Object>)input).forEach(item -> {
        if (and_append.get()) {
          sqlbuf.append(" and ");
        } else {
          and_append.set(true);
        }
        if (item instanceof JdbcQueryRequest) {
          JdbcQueryStatement sts = provider.prepareQuerySelect((JdbcQueryRequest)item);
          sqlbuf.append("(").append(sts.getSql()).append(")");
          if (sts.hasParameter()) {
            parameters.addAll(sts.getParameters());
          }
        } else {
          sqlbuf.append("?");
          parameters.add(provider.getTypeConverterFactory().convertObject(item, type));
        }
      });
    } else if (input.getClass().isArray()) {
      for (int idx_input = 0; idx_input < Array.getLength(input); idx_input++) {
        if (idx_input > 0) {
          sqlbuf.append(" and ");
        }
        Object item = Array.get(input, idx_input);
        if (item instanceof JdbcQueryRequest) {
          JdbcQueryStatement sts = provider.prepareQuerySelect((JdbcQueryRequest)item);
          sqlbuf.append("(").append(sts.getSql()).append(")");
          if (sts.hasParameter()) {
            parameters.addAll(sts.getParameters());
          }
        } else {
          sqlbuf.append("?");
          parameters.add(provider.getTypeConverterFactory().convertObject(item, type));
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static void buildMoreParams(SQLDialectProvider provider, String column, WhereCloumn.Condition condition, ConditionOperation operation, JDBCType type, StringBuffer sqlbuf, List<Object> parameters) {
    if (condition.getValue() instanceof JdbcQueryRequest) {
      JdbcQueryStatement sts = provider.prepareQuerySelect((JdbcQueryRequest)condition.getValue());
      sqlbuf.append("\"").append(column).append("\" ")
        .append(operation.operator()).append(" (")
        .append(sts.getSql()).append(")");
    } else {
      sqlbuf.append("\"").append(column).append("\" ").append(operation.operator()).append(" ");
      Object input = condition.getValue();
      if (input instanceof Iterable) {
        AtomicBoolean and_append = new AtomicBoolean(false);
        ((Iterable<Object>)input).forEach(item -> {
          if (and_append.get()) {
            sqlbuf.append(", ");
          } else {
            and_append.set(true);
          }
          if (item instanceof JdbcQueryRequest) {
            JdbcQueryStatement sts = provider.prepareQuerySelect((JdbcQueryRequest)item);
            sqlbuf.append("(").append(sts.getSql()).append(")");
            if (sts.hasParameter()) {
              parameters.addAll(sts.getParameters());
            }
          } else {
            sqlbuf.append("?");
            parameters.add(provider.getTypeConverterFactory().convertObject(item, type));
          }
        });
      } else if (input.getClass().isArray()) {
        for (int idx_input = 0; idx_input < Array.getLength(input); idx_input++) {
          if (idx_input > 0) {
            sqlbuf.append(", ");
          }
          Object item = Array.get(input, idx_input);
          if (item instanceof JdbcQueryRequest) {
            JdbcQueryStatement sts = provider.prepareQuerySelect((JdbcQueryRequest)item);
            sqlbuf.append("(").append(sts.getSql()).append(")");
            if (sts.hasParameter()) {
              parameters.addAll(sts.getParameters());
            }
          } else {
            sqlbuf.append("?");
            parameters.add(provider.getTypeConverterFactory().convertObject(item, type));
          }
        }
      }
    }
  }

  public JdbcQueryStatement buildSQLStatement(SQLDialectProvider provider) {
    StringBuffer sqlbuf = new StringBuffer();
    List<Object> parameters = new LinkedList<>();
    if (StringUtils.isBlank(this.column)) {
      Validate.isTrue(this.childrens != null && this.childrens.size() > 0, "子条件不能为空");
      boolean where_append = false;
      for (WhereCloumn where : this.childrens) {
        if (where_append) {
          if (this.testOr()) {
            sqlbuf.append(" or ");
          } else {
            sqlbuf.append(" and ");
          }
        } else {
          where_append = true;
        }
        JdbcQueryStatement sts = where.buildSQLStatement(provider);
        sqlbuf.append(sts.getSql());
        if (sts.hasParameter()) {
          parameters.addAll(sts.getParameters());
        }
      }
    } else {
      String column_name_all = StringUtils.replace(this.column, ",", "|");
      column_name_all = StringUtils.replace(column_name_all, ";", "|");
      String column_names[] = StringUtils.split(column_name_all, "|");
      for (int xi = 0; xi < column_names.length; xi++) {
        if (xi > 0) {
          sqlbuf.append(" or ");
        }
        String column = column_names[xi];
        if (this.condition != null) {
          ConditionOperation operation = this.condition.getOperation();
          if (operation.parameter_count() == 0) {
            sqlbuf.append("\"").append(column).append("\" ").append(operation.operator());
          } else if (operation.parameter_count() == 1) {
            buildOneParams(provider, column, condition, operation, type, sqlbuf, parameters);
          } else if (operation.parameter_count() == 2) {
            buildTwoParams(provider, column, condition, operation, type, sqlbuf, parameters);
          } else {
            buildMoreParams(provider, column, condition, operation, type, sqlbuf, parameters);
          }
        } else {
          sqlbuf.append("(");
          boolean where_append = false;
          for (WhereCloumn.Condition condition : this.conditions) {
            if (where_append) {
              if (this.testOr()) {
                sqlbuf.append(" or ");
              } else {
                sqlbuf.append(" and ");
              }
            } else {
              where_append = true;
            }
            ConditionOperation operation = condition.getOperation();
            if (condition.hasColumn()) {
              column = condition.getColumn();
            }
            if (operation.parameter_count() == 0) {
              sqlbuf.append("\"").append(column).append("\" ").append(operation.operator());
            } else if (operation.parameter_count() == 1) {
              buildOneParams(provider, column, condition, operation, type, sqlbuf, parameters);
            } else if (operation.parameter_count() == 2) {
              buildTwoParams(provider, column, condition, operation, type, sqlbuf, parameters);
            } else {
              buildMoreParams(provider, column, condition, operation, type, sqlbuf, parameters);
            }
          }
          sqlbuf.append(")");
        }
      }
    }
    return new JdbcQueryStatement(sqlbuf.toString(), parameters);
  }


  @Data
  public static class Condition implements Serializable {

    private String column;

    private ConditionOperation operation;

    private Object value;

    @JsonIgnore
    private Object array;

    public static Condition of(String column) {
      Condition cod = new Condition();
      cod.setColumn(column);
      return cod;
    }

    public boolean hasColumn() {
      return StringUtils.isNotBlank(this.column);
    }

    @JsonIgnore
    public int getArray_length() {
      if (array == null) return 0;
      return Array.getLength(array);
    }

  }

}