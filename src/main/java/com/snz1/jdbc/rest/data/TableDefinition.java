package com.snz1.jdbc.rest.data;

import java.io.Serializable;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import gateway.sc.v2.User;
import lombok.Data;

// 数据表访问权限定义规则，无定义的表则都可以查询
@Data
public class TableDefinition implements Serializable {

  // 数据表名称
  private String table_name;

  // 数据表别名
  private String alias_name;

  // 是否有别名
  public boolean hasAlias_name() {
    return StringUtils.isNotBlank(alias_name);
  }

  // 所属应用ID字段
  // 定义了此字段则表查询或关联查询将以当前请求应用ID作为限定范围
  private String owner_app_id;

  // 所属应用名称字段
  private String owner_app_name;

  // 所属用户ID字段
  // 定义了此字段则表查询或关联查询将以当前请求用户ID作为限定范围
  private TableDefinition.UserIdColumn owner_user_id;

  // 所属用户称呼
  private String owner_user_compellation;

  // 创建时间字段
  private String created_time_column;

  // 创建用户ID字段
  private TableDefinition.UserIdColumn created_user_id;

  // 创建用户称呼字段
  private String created_user_compellation;

  // 更新时间字段
  private String updated_time_column;

  // 更新用户ID字段
  private TableDefinition.UserIdColumn updated_user_id;

  // 更新用户称呼字段
  private String updated_user_compellation;

  // 字段操作授权定义
  private Map<String, ColumnDefinition> column_definition;

  // 是否自定定义
  public boolean hasColumn_definition() {
    return this.column_definition != null && this.column_definition.size() > 0;
  }

  // 缓存策略
  private CachePolicy cache_policy;

  // 是否有缓存策略
  public boolean hasCache_policy() {
    return this.cache_policy != null;
  }

  // 缓存策略
  @Data
  public static class CachePolicy implements Serializable {

    // 缓存行
    private Boolean row;

    // 缓存查询
    private Boolean query;

    // 缓存总计
    private Boolean count;

  }

  // 用户标识字段
  @Data
  public static class UserIdColumn implements Serializable {

    // 字段名
    private String column;

    // ID类型
    private User.IdType idtype;

  }

  // 字段操作权限配置
  @Data
  public static class ColumnDefinition implements Serializable {

    // 字段定义
    private String column;

    // 查询权限SP-EL定义
    private String query;

    // 更新权限SP-EL定义
    private String update;

    public void validate() throws Exception {
      // TODO：校验SPEL
    }

  }

  public void validate() throws Exception {
    Validate.notBlank(this.getTable_name(), "数据表名不能为空");
    if (this.hasColumn_definition()) {
      for (Map.Entry<String, ColumnDefinition> kv : this.getColumn_definition().entrySet()) {
        String k = kv.getKey();
        Validate.notNull(kv.getKey(), "字段名不能为空");
        ColumnDefinition v = kv.getValue();
        v.setColumn(k);
        v.validate();
      }
    }
  }

}
