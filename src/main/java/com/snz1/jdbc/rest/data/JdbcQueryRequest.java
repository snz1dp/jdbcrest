package com.snz1.jdbc.rest.data;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import lombok.Data;

@Data
public class JdbcQueryRequest implements Serializable {

  // 请求ID
  private String request_id;

  // 查询结果描述
  private ResultMeta result_meta = new ResultMeta();

  // 返回描述
  @Data
  public static class ResultMeta {

    // 返回所有字段
    private boolean all_columns = true;

    // 字段
    private Map<String, ResultColumn> columns = new HashMap<String, ResultColumn>();

    // 是否包含元信息
    private boolean contain_meta = false;

    // 对象结构
    private ResultObjectStruct row_struct = ResultObjectStruct.map;

    // 仅一列则返回紧凑模式
    private boolean column_compact;

    // 开始索引
    private long offset;

    // 返回限制
    private long limit;

    // 列
    @Data
    public static class ResultColumn implements Serializable {

      // 字段名
      private String name;

      // 别名
      private String alias;

      private ColumnType type = ColumnType.raw;

      public static ResultColumn of(String name) {
        ResultColumn ret = new ResultColumn();
        ret.setName(name);
        return ret;
      }

      public boolean hasAlias() {
        return alias != null;
      }

    }

    // 列类型
    public static enum ColumnType {

      raw,

      map,

      list,

      base64;

    }

    // 返回的对象数据结构
    public static enum ResultObjectStruct {

      // 返回对象
      map("对象"),
      
      // 返回列表
      list("列表");

      private String title;

      private ResultObjectStruct(String title) {
        this.title = title;
      }

      public String title() {
        return title;
      }

    }

  }

}
