package com.snz1.jdbc.rest.data;

import gateway.api.Return;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@EqualsAndHashCode(callSuper=true)
@ToString(callSuper = true)
public class JdbcQueryResponse<T> extends Return<T> {

  private TableMeta meta;

  public void setData(T d) {
    this.data = d;
  }

  public T getData() {
    return this.data;
  }

}
