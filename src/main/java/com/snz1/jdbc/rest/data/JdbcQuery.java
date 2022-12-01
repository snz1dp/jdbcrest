package com.snz1.jdbc.rest.data;

import java.io.Serializable;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class JdbcQuery implements Serializable {

  private String sql;

  private List<Object> parameters;

  public boolean hasParameter() {
    return this.parameters != null && this.parameters.size() > 0;
  }

}
