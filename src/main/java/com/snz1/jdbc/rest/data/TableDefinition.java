package com.snz1.jdbc.rest.data;

import java.io.Serializable;

import gateway.sc.v2.User;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

// 数据表访问权限定义规则，无定义的表则都可以查询
@Data
public class TableDefinition implements Serializable {

  // 数据表名称
  private String table_name;

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

  // 表级操作授权定义
  private AuthorizeAccess table_authorization[];

  // 字段操作授权定义
  private FieldAuthorizeAccess column_authorization[];

  // 用户标识字段
  @Data
  public static class UserIdColumn implements Serializable {

    // 字段名
    private String column;

    // ID类型
    private User.IdType idtype;

  }

  @Data
  public static class AuthorizeAccess implements Serializable {

    // 访问权限
    private AccessPermission permission;

    // 权限表达式，Spring-EL表达式
    private String authorize;

  }

  @Data
  @ToString(callSuper = true)
  @EqualsAndHashCode(callSuper = true)
  public static class FieldAuthorizeAccess extends AuthorizeAccess {

    private String column;

  }

  // 访问权限
  public static enum AccessPermission {

    query,

    insert,

    update,

    delete,

    ;

  }

}
