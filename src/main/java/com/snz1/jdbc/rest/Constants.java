package com.snz1.jdbc.rest;

public abstract class Constants {

  // 返回列请求参数
  public static final String RESULT_ALL_COLUMNS_ARG = "_result.all_column";

  // 返回列请求参数
  public static final String RESULT_COLUMNS_ARG = "_result.column";

  // 返回包含元信息参数
  public static final String RESULT_CONTAIN_META_ARG = "_result.contain_meta";

  // 返回行结构参数
  public static final String RESULT_ROW_STRUCT_ARG = "_result.row_struct";

  // 开始索引
  public static final String OFFSET_ARG = "offset";

  // 返回数量
  public static final String LIMIT_ARG = "limit";

  // 最大返回数量
  public static final int DEFAULT_MAX_LIMIT = 1000;

}
