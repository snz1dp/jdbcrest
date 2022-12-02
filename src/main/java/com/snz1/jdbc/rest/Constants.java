package com.snz1.jdbc.rest;

public abstract class Constants {

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

  // 开始索引
  public static final String OFFSET_ARG = "offset";

  // 返回数量
  public static final String LIMIT_ARG = "limit";

  // 最大返回数量
  public static final int DEFAULT_MAX_LIMIT = 1000;

}
