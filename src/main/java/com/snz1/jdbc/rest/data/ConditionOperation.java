package com.snz1.jdbc.rest.data;

public enum ConditionOperation {

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