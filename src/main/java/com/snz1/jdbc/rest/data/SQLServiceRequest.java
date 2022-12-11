package com.snz1.jdbc.rest.data;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@EqualsAndHashCode
@ToString
public class SQLServiceRequest implements Serializable {

  // 更新时间
  @Getter
  private Date request_time = new Date();

  @Getter
  private SQLServiceDefinition definition;

  // 输入数据
  @Setter
  @Getter
  private Object input_data;
  
  @JsonIgnore
  private List<Map<String, Object>> _input_list;

  // 是单数据
  public boolean testSignletonData() {
    if (this.input_data == null) return true;
    return !(this.input_data instanceof List);
  }

  @JsonIgnore
  @SuppressWarnings("unchecked")
  public List<Map<String, Object>> getInput_list() {
    if (this._input_list != null) return this._input_list;
    if (this.getInput_data() instanceof List) {
      return this._input_list = (List<Map<String, Object>>)this.getInput_data();
    } else {
      return this._input_list = Arrays.asList((Map<String, Object>)this.getInput_data());
    }
  }

  @JsonIgnore
  public Map<String, Object> getInput_map() {
    return this.getInput_list().get(0);
  }

  public static SQLServiceRequest of(SQLServiceDefinition def) {
    SQLServiceRequest request = new SQLServiceRequest();
    request.definition = def;
    return request;
  }

}
