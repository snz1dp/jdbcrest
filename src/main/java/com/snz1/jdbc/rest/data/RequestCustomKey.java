package com.snz1.jdbc.rest.data;

import java.io.Serializable;
import java.util.List;

import lombok.Data;

@Data
public class RequestCustomKey implements Serializable {
    
  // 主键分割符
  private String key_splitter;

  // 自定义主键
  private Object custom_key;

  // 是否有自定义主键
  public boolean hasCustom_key() {
    return this.custom_key != null;
  }

  // 是否有分割符
  public boolean hasKey_splitter() {
    return this.testComposite_key() && this.key_splitter != null;
  }

  // 是否符合主键
  public boolean testComposite_key() {
    if (this.custom_key != null) {
      return this.custom_key instanceof List;
    }
    return false;
  }

}
