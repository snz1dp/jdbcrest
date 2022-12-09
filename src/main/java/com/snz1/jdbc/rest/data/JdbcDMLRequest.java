package com.snz1.jdbc.rest.data;

import java.io.Serializable;

import lombok.Data;

@Data
public class JdbcDMLRequest implements Serializable {

  private ManipulationRequest[] insert;

  private ManipulationRequest[] update;

  private ManipulationRequest[] delete;

}
