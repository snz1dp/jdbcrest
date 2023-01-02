package com.snz1.jdbc.rest.data;

import java.io.Serializable;
import java.util.Date;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class LicenseMeta implements Serializable {

  private Date end;

  private String hint;

  private String consumer;

  private String provider;

  private String signature;

}
