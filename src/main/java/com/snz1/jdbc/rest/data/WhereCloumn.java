package com.snz1.jdbc.rest.data;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.sql.JDBCType;
import java.util.LinkedList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.snz1.jdbc.rest.data.JdbcQueryRequest.ConditionOperation;
import com.snz1.jdbc.rest.utils.JdbcUtils;

import lombok.Data;

@Data
public class WhereCloumn implements Serializable {

  private String column;

  private WhereCloumn.Condition condition;

  private JDBCType type;

  private List<WhereCloumn.Condition> conditions;

  public static WhereCloumn of(String name) {
    WhereCloumn where_column = new WhereCloumn();
    where_column.setColumn(name);
    return where_column;
  }

  public void addCondition(ConditionOperation operation, String val) {
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
      for (WhereCloumn.Condition condition : this.conditions) {
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
          if (condition.getArray() == null) {
            condition.setArray(JdbcUtils.toArray(condition.getValue(), this.type));
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
        parameters.add(JdbcUtils.convert(this.condition.getValue(), this.type));
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
      for (WhereCloumn.Condition condition : this.conditions) {
        ConditionOperation operation = condition.getOperation();
        if (operation.parameter_count() == 1) {
          parameters.add(JdbcUtils.convert(this.condition.getValue(), this.type));
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

    private Object value;

    @JsonIgnore
    private Object array;

    @JsonIgnore
    public int getArray_length() {
      if (array == null) return 0;
      return Array.getLength(array);
    }

  }

}