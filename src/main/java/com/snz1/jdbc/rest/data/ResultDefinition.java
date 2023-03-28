package com.snz1.jdbc.rest.data;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import lombok.Data;

// 返回定义
@Data
public class ResultDefinition implements Serializable {

  // 默认返回查询的字段
  private boolean all_column = true;

  // 单对象返回
  private boolean signleton = false;

  // 字段
  private Map<String, ResultDefinition.ResultColumn> columns = new HashMap<String, ResultDefinition.ResultColumn>();

  public boolean hasColumn() {
    return this.columns != null;
  }

  // 是否包含元信息
  private boolean contain_meta = false;

  // 是否包含行统计
  private boolean row_total = false;

  // 对象结构
  private ResultDefinition.ResultRowStruct row_struct = ResultRowStruct.map;

  // 仅一列则返回紧凑模式
  private boolean column_compact;

  // 开始索引
  private long offset;

  // 返回限制
  private long limit;

  // 列
  @Data
  public static class ResultColumn implements Serializable, Cloneable {

    // 字段名
    private String name;

    // 别名
    private String alias;

    private ResultDefinition.ResultType type = ResultType.raw;

    public static ResultDefinition.ResultColumn of(String name) {
      ResultDefinition.ResultColumn ret = new ResultColumn();
      ret.setName(name);
      return ret;
    }

    public boolean hasAlias() {
      return alias != null;
    }

    public ResultColumn clone() {
      try {
        return (ResultColumn)super.clone();
      } catch(CloneNotSupportedException e) {
        throw new IllegalStateException(e.getMessage(), e);
      }
    }

  }

  // 列类型
  public static enum ResultType {

    raw,

    map,

    list,

    set,

    base64;

  }

  // 返回的对象数据结构
  public static enum ResultRowStruct {

    // 返回对象
    map("对象"),
    
    // 返回列表
    list("列表");

    private String title;

    private ResultRowStruct(String title) {
      this.title = title;
    }

    public String title() {
      return title;
    }

  }

}
