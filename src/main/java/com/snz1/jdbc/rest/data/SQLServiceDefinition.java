package com.snz1.jdbc.rest.data;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.SqlCommandType;

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

  public boolean hasSql_fragments() {
    return this.sql_fragments != null && this.sql_fragments.size() > 0;
  }

  // SQL片段
  @Data
  public static class SQLFragment implements Serializable {

    // 索引号
    private String mapped_id;

    // SQL
    private String frangment_sql;

    // 最后一个
    private boolean last_fragment;

    // 返回类型
    private ResultDefinition result = new ResultDefinition();

    public boolean hasResult() {
      return result != null;
    }

    // SQL
    private MappedStatement mapped_statement;

    public SqlCommandType getCommand_type() {
      if (this.mapped_statement == null) return SqlCommandType.UNKNOWN;
      return this.getMapped_statement().getSqlCommandType();
    }

  }

  @Data
  public static class ResultDefinitionYamlWrapper {

    // 单对象返回
    private boolean signleton = false;

    // 仅一列则返回紧凑模式
    private boolean column_compact;

    // 字段
    private ResultDefinition.ResultColumn[] columns;

  }

}
