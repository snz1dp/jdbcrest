package com.snz1.jdbc.rest.data;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.sql.JDBCType;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.snz1.jdbc.rest.service.JdbcTypeConverterFactory;

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

  public String toWhereSQL(JdbcTypeConverterFactory factory) {
    if (StringUtils.isBlank(this.column)) {
      Validate.isTrue(this.childrens != null && this.childrens.size() > 0, "子条件不能为空");
      StringBuffer sqlbuf = new StringBuffer();
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
        sqlbuf.append(where.toWhereSQL(factory));
      }
      return sqlbuf.toString();
    } else {
      String column_name_all = StringUtils.replace(this.column, ",", "|");
      column_name_all = StringUtils.replace(column_name_all, ";", "|");
      String column_names[] = StringUtils.split(column_name_all, "|");
      StringBuffer sqlbuf = new StringBuffer();
      for (int xi = 0; xi < column_names.length; xi++) {
        if (xi > 0) {
          sqlbuf.append(" or ");
        }
        String column = column_names[xi];
        if (this.condition != null) {
          ConditionOperation operation = this.condition.getOperation();
          if (operation.parameter_count() == 0) {
            sqlbuf.append(String.format("\"%s\" %s", column, operation.operator()));
          } else if (operation.parameter_count() == 1) {
            sqlbuf.append(String.format("\"%s\" %s ?", column, operation.operator()));
          } else if (operation.parameter_count() == 2) {
            sqlbuf.append(String.format("\"%s\" %s ? and ?", column, operation.operator()));
          } else {
            condition.setArray(factory.convertArray(condition.getValue(), this.type));
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
            sqlbuf.append(String.format("\"%s\" %s (%s)", this.column, operation.operator(), parambuf.toString()));
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
            String column_name = this.column;
            if (condition.hasColumn()) {
              column_name = condition.getColumn();
            }
            if (operation.parameter_count() == 0) {
              sqlbuf.append(String.format("\"%s\" %s", column_name, operation.operator()));
            } else if (operation.parameter_count() == 1) {
              sqlbuf.append(String.format("\"%s\" %s ?", column_name, operation.operator()));
            } else if (operation.parameter_count() == 2) {
              sqlbuf.append(String.format("\"%s\" %s ? and ?", column_name, operation.operator()));
            } else {
              if (condition.getArray() == null) {
                condition.setArray(factory.convertArray(condition.getValue(), this.type));
              }
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
              sqlbuf.append(String.format("\"%s\" %s (%s)", column_name, operation.operator(), parambuf.toString()));
            }
          }
          sqlbuf.append(")");
        }
      }
      return sqlbuf.toString();
    }
  }

  public void buildParameters(List<Object> parameters, JdbcTypeConverterFactory factory) {
    if (StringUtils.isBlank(this.column)) {
      Validate.isTrue(this.childrens != null && this.childrens.size() > 0, "子条件不能为空");
      for (WhereCloumn where : this.childrens) {
        where.buildParameters(parameters, factory);
      }
    } else {
      String column_name_all = StringUtils.replace(this.column, ",", "|");
      column_name_all = StringUtils.replace(column_name_all, ";", "|");
      String column_names[] = StringUtils.split(column_name_all, "|");
      for (int xi = 0; xi < column_names.length; xi++) {
        if (this.condition != null) {
          ConditionOperation operation = condition.getOperation();
          if (operation.parameter_count() == 1) {
            parameters.add(factory.convertObject(condition.getValue(), this.type));
          } else if (operation.parameter_count() == 2) {
            Object data = factory.convertArray(condition.getValue(), this.type);
            parameters.add(Array.get(data, 0));
            parameters.add(Array.get(data, 0));
          } else if (operation.parameter_count() == 3) {
            if (condition.getArray() == null) {
              condition.setArray(factory.convertArray(condition.getValue(), this.type));
            }
            for (int i = 0; i < condition.getArray_length(); i++) {
              parameters.add(Array.get(condition.getArray(), i));
            }
          }
        } else {
          for (WhereCloumn.Condition condition : this.conditions) {
            ConditionOperation operation = condition.getOperation();
            if (operation.parameter_count() == 1) {
              parameters.add(factory.convertObject(condition.getValue(), this.type));
            } else if (operation.parameter_count() == 2) {
              Object data = factory.convertArray(condition.getValue(), this.type);
              parameters.add(Array.get(data, 0));
              parameters.add(Array.get(data, 0));
            } else if (operation.parameter_count() == 3) {
              if (condition.getArray() == null) {
                condition.setArray(factory.convertArray(condition.getValue(), this.type));
              }
              for (int i = 0; i < condition.getArray_length(); i++) {
                parameters.add(Array.get(condition.getArray(), i));
              }
            }
          }
        }
      }
    }
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