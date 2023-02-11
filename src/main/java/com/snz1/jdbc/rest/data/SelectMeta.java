package com.snz1.jdbc.rest.data;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import lombok.Data;

@Data
public class SelectMeta implements Serializable {

  // 是否去除重复
  private boolean distinct;

  // 统计
  private String count;

  // 查询字段
  private List<SelectMeta.SelectColumn> columns = new LinkedList<>();

  // 是否有总计
  public boolean hasCount() {
    return StringUtils.isNotBlank(this.count);
  }

  public boolean hasColumns() {
    return this.columns.size() > 0;
  }

  // 列
  @Data
  public static class SelectColumn implements Serializable {

    // 字段名
    private String column;

    // 函数
    private String function;

    // 定义AS
    private String as;

    public static SelectMeta.SelectColumn of(String name) {
      SelectMeta.SelectColumn ret = new SelectColumn();
      ret.setColumn(name);
      return ret;
    }

    public boolean hasAs() {
      return as != null;
    }

    public boolean hasFunction() {
      return function != null;
    }

  }

}