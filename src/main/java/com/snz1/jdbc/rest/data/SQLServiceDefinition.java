package com.snz1.jdbc.rest.data;

import java.io.Serializable;
import java.sql.JDBCType;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import lombok.Data;

// SQL转服务定义
@Data
public class SQLServiceDefinition implements Serializable {
  
  // 服务路径，从SQL文件名中获取
  private String service_path;

  // 文件位置
  private String file_location;

  // SQL片段列表
  private List<SQLFragment> sql_fragments = new LinkedList<SQLFragment>();

  // SQL片段
  @Data
  public static class SQLFragment implements Serializable {

    // SQL
    private String frangment_sql;

    // 参数
    private Map<String, JDBCType> parameter_map = new LinkedHashMap<>();

    // 最后一个
    private boolean last_fragment;

    // 返回类型
    private ResultDefinition.ResultType result_type;

    // 是否有参数
    public boolean hasParameters() {
      return parameter_map != null && parameter_map.size() > 0;
    }

  }

}
