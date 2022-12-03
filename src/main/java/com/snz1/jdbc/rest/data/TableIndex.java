package com.snz1.jdbc.rest.data;

import java.io.Serializable;

import lombok.Data;

@Data
public class TableIndex implements Serializable {
  
  private String name;

  private boolean unique;

  private String column;

  private String order;

  private Integer type;

}
