package com.snz1.jdbc.rest.data;

import java.io.Serializable;

import lombok.Data;

@Data
public class SQLClauses implements Serializable {
  
  // 注释
  private StringBuffer note = new StringBuffer();

  // SQL
  private StringBuffer sql = new StringBuffer();

}
