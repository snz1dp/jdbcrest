package com.snz1.jdbc.rest.data;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import lombok.Data;

@Data
public class JdbcDMLResponse implements Serializable {
    
  private List<Object> insert;

  private List<Object> update;

  private List<Object> delete;

  public JdbcDMLResponse addInsert(Object result) {
    if (this.insert == null) this.insert = new LinkedList<Object>();
    this.insert.add(result);
    return this;
  }

  public JdbcDMLResponse addUpdate(Object result) {
    if (this.update == null) this.update = new LinkedList<Object>();
    this.update.add(result);
    return this;
  }

  public JdbcDMLResponse addDelete(Object result) {
    if (this.delete == null) this.delete = new LinkedList<Object>();
    this.delete.add(result);
    return this;
  }

}
