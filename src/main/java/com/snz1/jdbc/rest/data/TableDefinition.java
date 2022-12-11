package com.snz1.jdbc.rest.data;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import gateway.sc.v2.User;
import lombok.Data;

// 数据表访问权限定义规则，无定义的表则都可以查询
@Data
public class TableDefinition implements Serializable {

  // 数据表名称
  private String name;

  // 数据表别名（实际名称)
  private String alias;

  // 是否有别名
  public boolean hasAlias_name() {
    return StringUtils.isNotBlank(alias);
  }

  // 获取实际表名
  public String resolveName() {
    return this.hasAlias_name() ? this.getAlias() : this.getName();
  }

  // 所属用户ID字段
  // 定义了此字段则表查询或关联查询将以当前请求用户ID作为限定范围
  private TableDefinition.UserIdColumn owner_id_column;

  // 是否用户所属字段
  public boolean hasOwner_id_column() {
    return this.owner_id_column != null && StringUtils.isNotBlank(this.owner_id_column.getName());
  }

  // 缺省条件
  private List<WhereCloumn> default_where = new LinkedList<>();

  // 是否有缺省条件
  public boolean hasDefault_where() {
    return this.default_where != null && this.default_where.size() > 0;
  }

  public List<WhereCloumn> copyDefault_where() {
    final List<WhereCloumn> ret = new LinkedList<>();
    this.default_where.forEach(w -> {
      ret.add(w.clone());
    });
    return ret;
  }

  // 创建时间字段
  private String created_time_column;

  public boolean hasCreated_time_column() {
    return StringUtils.isNotBlank(this.created_time_column);
  }

  // 创建用户ID字段
  private TableDefinition.UserIdColumn creator_id_column;

  public boolean hasCreator_id_column() {
    return this.creator_id_column != null && StringUtils.isNotBlank(this.creator_id_column.getName());
  }

  // 创建用户称呼字段
  private String creator_name_column;

  public boolean hasCreator_name_column() {
    return StringUtils.isNotBlank(this.creator_name_column);
  }

  // 更新时间字段
  private String updated_time_column;

  public boolean hasUpdated_time_column() {
    return StringUtils.isNotBlank(this.updated_time_column);
  }

  // 更新用户ID字段
  private TableDefinition.UserIdColumn mender_id_column;

  public boolean hasMender_id_column() {
    return this.mender_id_column != null && StringUtils.isNotBlank(this.mender_id_column.getName());
  }

  // 更新用户称呼字段
  private String mender_name_column;

  public boolean hasMender_name_column() {
    return StringUtils.isNotBlank(this.mender_name_column);
  }

  public boolean inColumn(String name) {
    if (this.hasCreated_time_column() &&
      StringUtils.equalsIgnoreCase(name, this.getCreated_time_column())
    ) return true;
    if (this.hasCreator_id_column() &&
      StringUtils.equalsIgnoreCase(name, this.getCreator_id_column().getName())
    ) return true;
    if (this.hasCreator_name_column() &&
      StringUtils.equalsIgnoreCase(name, this.getCreator_name_column())
    ) return true;
    if (this.hasOwner_id_column() &&
      StringUtils.equalsIgnoreCase(name, this.getOwner_id_column().getName())
    ) return true;
    if (this.hasUpdated_time_column() &&
      StringUtils.equalsIgnoreCase(name, this.getUpdated_time_column())
    ) return true;
    if (this.hasMender_id_column() &&
      StringUtils.equalsIgnoreCase(name, this.getMender_id_column().getName())
    ) return true;
    if (this.hasMender_name_column() &&
      StringUtils.equalsIgnoreCase(name, this.getMender_name_column())
    ) return true;
    return false;
  }

  // 用户标识字段
  @Data
  public static class UserIdColumn implements Serializable {

    // 字段名
    private String name;

    // ID类型
    private User.IdType idtype = User.IdType.uname;

  }

  public void validate() throws Exception {
    Validate.notBlank(this.getName(), "数据表名不能为空");
  }

}