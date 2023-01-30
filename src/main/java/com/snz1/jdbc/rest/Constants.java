package com.snz1.jdbc.rest;

public abstract class Constants {

  // 无应用权代码
  public static final String [] NO_APP_CODES = new String[] {""};

  // SELECT参数
  public static final String SELECT_ARG = "_select";

  // 排序参数
  public static final String ORDERBY_ARG = "_order";

  // 分组参数
  public static final String GROUPBY_ARG ="_groupby";

  // 统计参数
  public static final String COUNT_ARG = "_count";

  // 去除重行参数
  public static final String DISTINCT_ARG =  "_distinct";

  // 关联查询
  public static final String JOIN_ARG =  "_join";

  // 返回列请求参数
  public static final String RESULT_ALL_COLUMNS_ARG = "_result.all_column";

  // 返回列请求参数
  public static final String RESULT_COLUMNS_ARG = "_result.column";

  // 返回包含元信息参数
  public static final String RESULT_CONTAIN_META_ARG = "_result.contain_meta";

  // 返回行结构参数
  public static final String RESULT_ROW_STRUCT_ARG = "_result.row_struct";

  // 单对象返回
  public static final String RESULT_SIGNLETON_ARG = "_result.signleton";

  // 分页是否缺省返回统计
  public static final String RESULT_TOTAL_ARG = "_result.total";

  // 开始索引
  public static final String OFFSET_ARG = "offset";

  // 返回数量
  public static final String LIMIT_ARG = "limit";

  // 最大返回数量
  public static final int DEFAULT_MAX_LIMIT = 1000;

  // 主键请求头
  public static final String HEADER_PRIMARY_KEY_ARG = "jdbcrest-primary-key";

  // 主键分割请求头
  public static final String HEADER_KEY_SPLITTER_ARG = "jdbcrest-key-splitter";

  // 缺省主键分割符
  public static final String DEFAULT_KEY_SPLITTER = "|";

  // 请求头修改模式
  public static final String HEADER_UPDATE_MODE_ARG = "jdbcrest-update-mode";

  // 补丁模式
  public static final String UPDATE_PATCH_MODE = "patch";

  // 条件前缀
  public static final String HEADER_WHERE_PREFIX = "jdbcrest-";

  // SQL分割
  public static final String SQL_SPLITTER = ";";

  // SQL参数开始
  public static final String SQL_PARAM_PREFIX = "#{";

  // SQL参数结束
  public static final String SQL_PARAM_SUFFIX = "}";

  // Schemas缓存
  public static final String SCHEMAS_CACHE = "schemas";

  // Metadata缓存
  public static final String METADATA_CACHE = "metadata";

  // Catalogs缓存
  public static final String CATALOGS_CACHE = "catalogs";

  // Table缓存
  public static final String TABLES_CACHE = "tables";

  // 是否存在缓存
  public static final String EXISTED_CACHE = "existed";

  // 主键缓存
  public static final String PRIMARYKEY_CACHE = "primarykey";

  // TableMeta缓存
  public static final String TABLEMETA_CACHE = "tablemeta";

  // 授权代码
  public static final String LICENSE_CODE_ARG = "license.code";

}
