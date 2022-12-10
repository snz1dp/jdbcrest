package com.snz1.jdbc.rest.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ResultMap;
import org.apache.ibatis.mapping.ResultMapping;
import org.apache.ibatis.mapping.SqlCommandType;
import org.apache.ibatis.scripting.defaults.RawSqlSource;
import org.apache.ibatis.session.Configuration;
import org.postgresql.jdbc.PgArray;

import com.snz1.jdbc.rest.Constants;
import com.snz1.jdbc.rest.data.JdbcMetaData;
import com.snz1.jdbc.rest.data.ResultDefinition;
import com.snz1.jdbc.rest.data.SQLClauses;
import com.snz1.utils.JsonUtils;

public abstract class JdbcUtils extends org.springframework.jdbc.support.JdbcUtils {

  public static JdbcMetaData getJdbcMetaData(Connection conn) throws SQLException {
    JdbcMetaData temp_meta = new JdbcMetaData();
    DatabaseMetaData table_meta =  conn.getMetaData();
    temp_meta.setProduct_name(table_meta.getDatabaseProductName());
    temp_meta.setProduct_version(table_meta.getDatabaseProductVersion());
    temp_meta.setDriver_name(table_meta.getDriverName());
    temp_meta.setDriver_version(table_meta.getDriverVersion());
    return temp_meta;
  }

  public static Object getResultSetValue(ResultSet rs, int index) throws SQLException {
    Object ret = org.springframework.jdbc.support.JdbcUtils.getResultSetValue(rs, index);
    if (ret != null) {
      String clazz_name = ret.getClass().getName();
      if (!StringUtils.startsWithAny(clazz_name, "java.")) {
        if (ret instanceof PgArray) {
          ret = ((PgArray)ret).getArray();
        }
      }
    }
    return ret;
  }

  @SuppressWarnings("unchecked")
  public static Object listToArray(List<?> list, JDBCType type) {
    if (type == null) return ((List<Object>)list).toArray(new Object[0]);
    List<Object> retlst = new LinkedList<>();
    list.forEach(o -> {
      retlst.add(convert(o, type));
    });
    return retlst.toArray(new Object[0]);
  }

  public static Object jsonToArray(String val, JDBCType type) {
    List<?> list = JsonUtils.fromJson(val, List.class);
    if (list.size() == 0) return null;
    return listToArray(list, type);
  }

  public static Object splitToArray(String val, JDBCType type) {
    if (type == null) {
      return StringUtils.split(val, ',');
    } else {
      List<String> list = Arrays.asList(StringUtils.split(val, ','));
      return listToArray(list, type);
    }
  }

  public static Object toArray(Object val, JDBCType type) {
    if (val instanceof String) {
      if (StringUtils.startsWith((String)val, "[")) {
        return jsonToArray((String)val, type);
      } else {
        return splitToArray((String)val, type);
      }
    } else if (val instanceof Set) {
      return ((Set<?>)val).toArray(new Object[0]);
    } else if (val instanceof List) {
      return ((List<?>)val).toArray(new Object[0]);
    } else if (val instanceof Array) {
      return val;
    } else {
      return new Object[] { val };
    }
  }

  public static Object convert(Object input, JDBCType type) {
    if (input == null || type == null) {
      return input;
    }
    // TODO: 数据类型转换不尽如意
    if (type == JDBCType.INTEGER ||
      type == JDBCType.SMALLINT ||
      type == JDBCType.TINYINT ||
      type == JDBCType.BIT ||
      type == JDBCType.BIGINT) {
      if (input instanceof Double) {
        return ((Double)input).longValue();
      } else if (input instanceof Float) {
        return ((Float)input).longValue();
      } else if (input instanceof String) {
        return Integer.parseInt((String)input);
      }
      return input;
    } else if (type == JDBCType.DECIMAL ||
      type == JDBCType.NUMERIC ||
      type == JDBCType.REAL ||
      type == JDBCType.FLOAT ||
      type == JDBCType.DOUBLE) {
      if (input instanceof Number) {
        return input;
      } if (input instanceof String) {
        return Double.parseDouble((String)input);
      }
      return input;
    } else if (type == JDBCType.DATE) {
      if (input instanceof String) {
        try {
          return new java.sql.Date(new SimpleDateFormat("yyyy-MM-dd").parse((String)input).getTime());
        } catch(ParseException e) {
          throw new IllegalArgumentException(e.getMessage(), e);
        }
      } else if (input instanceof java.sql.Date) {
        return input;
      } else if (input instanceof Date) {
        return new java.sql.Date(((Date)input).getTime());
      } else if (input instanceof Double) {
        return new java.sql.Date(((Double)input).longValue());
      } else if (input instanceof Float) {
        return new java.sql.Date(((Float)input).longValue());
      } else if (input instanceof Long) {
        return new java.sql.Date((Long)input);
      } else if (input instanceof Integer) {
        return new java.sql.Date((Integer)input);
      }
      return input;
    } else if (type == JDBCType.TIME || type == JDBCType.TIME_WITH_TIMEZONE) {
      if (input instanceof String) {
        try {
          return new SimpleDateFormat("HH:mm:ss").parse((String)input);
        } catch(ParseException e1) {
          try {
            return new SimpleDateFormat("HH:mm").parse((String)input);
          } catch(ParseException e2) {
            throw new IllegalArgumentException(e2.getMessage(), e2);
          }
        }
      } if (input instanceof Date) {
        return new java.sql.Time(((Date)input).getTime());
      } else if (input instanceof Time) {
        return input;
      } else if (input instanceof Double) {
        return new Time(((Double)input).longValue());
      } else if (input instanceof Float) {
        return new Time(((Float)input).longValue());
      } else if (input instanceof Long) {
        return new Time((Long)input);
      } else if (input instanceof Integer) {
        return new Time((Integer)input);
      }
      return input;
    } else if (type == JDBCType.TIMESTAMP || type == JDBCType.TIMESTAMP_WITH_TIMEZONE) {
      if (input instanceof String) {
        try {
          return new SimpleDateFormat(JsonUtils.JsonDateFormat).parse((String)input);
        } catch(ParseException e1) {
          try {
            return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").parse((String)input);
          } catch(ParseException e2) {
            throw new IllegalArgumentException(e2.getMessage(), e2);
          }
        }
      } else if (input instanceof Date) {
        return new java.sql.Timestamp(((Date)input).getTime());
      } else if (input instanceof Double) {
        return new Date(((Double)input).longValue());
      } else if (input instanceof Float) {
        return new Date(((Float)input).longValue());
      } else if (input instanceof Long) {
        return new Date((Long)input);
      } else if (input instanceof Integer) {
        return new Date((Integer)input);
      }
      return input;
    } else if (type == JDBCType.BOOLEAN) {
      if (input instanceof String) {
        return Boolean.parseBoolean((String)input);
      }
      return input;
    } else if (type == JDBCType.BLOB) {
      if (input instanceof String) {
        return Base64.decodeBase64((String)input);
      }
      return input;
    }
    return input;
  }

	public static List<SQLClauses> loadSQLClausesFromFile(File file, String batch_splitter, Charset defaultCharset) throws IOException {
    List<SQLClauses> sql_batchs = new LinkedList<SQLClauses>();
    List<String> filelines = readURLToLines(file, defaultCharset);
    SQLClauses sql_clauses = new SQLClauses();

    boolean in_annotation = false;
    for (String line : filelines) {
      if (in_annotation) {
        in_annotation = !StringUtils.trim(line).endsWith("*/");
        if (in_annotation) {
          sql_clauses.getNote().append(line);
          sql_clauses.getNote().append("\n");
        } else {
          sql_clauses.getNote().append(StringUtils.stripEnd(line, " ").substring(0, line.length() - 2));
        }
        continue;
      } else if (StringUtils.trim(line).startsWith("/*")) {
        in_annotation = !StringUtils.trim(line).endsWith("*/");
        if (in_annotation) {
          sql_clauses.getNote().append(StringUtils.stripEnd(line, " ").substring(2));
        } else {
          sql_clauses.getNote().append(StringUtils.stripEnd(line, " ").substring(2, StringUtils.stripEnd(line, " ").length() - 2));
        }
        continue;
      } else if (StringUtils.trim(line).startsWith("--")) {
        sql_clauses.getNote().append(StringUtils.stripStart(line, " ").substring(2));
        continue;
      }

      if (line.endsWith(batch_splitter)) {
        sql_clauses.getSql().append(line.substring(0, line.length() - batch_splitter.length()));
        sql_batchs.add(sql_clauses);
        sql_clauses = new SQLClauses();
      } else {
        sql_clauses.getSql().append(line);
        sql_clauses.getSql().append("\n");
      }
    }
    if (StringUtils.isNotBlank(sql_clauses.getSql().toString())) {
      sql_batchs.add(sql_clauses);
    }
    return new ArrayList<SQLClauses>(sql_batchs);
  }

  public static List<String> readURLToLines(File url, Charset defaultCharset) throws IOException {
    InputStream in = new FileInputStream(url);
    try {
      return IOUtils.readLines(in, defaultCharset);
    } finally {
      IOUtils.closeQuietly(in);
    }
  }

  // 分析SQL参数
  public static Map<String, JDBCType> parseSQLParamters(String sql) {
    Map<String, JDBCType> paramters = new LinkedHashMap<>();
    return parseSQLParamters(sql, paramters);
  }

  // 分析SQL参数
  public static Map<String, JDBCType> parseSQLParamters(String sql, Map<String, JDBCType> paramters) {
    int index_start = 1;
    int param_start = sql.indexOf(Constants.SQL_PARAM_PREFIX, index_start);
    while(param_start > 0) {
      int param_end = sql.indexOf(Constants.SQL_PARAM_SUFFIX, param_start);
      if (param_end < 0) break;
      String param_content = sql.substring(param_start + Constants.SQL_PARAM_PREFIX.length(), param_end);
      String param_array[] = StringUtils.split(param_content, ",");
      if (param_array == null || param_array.length == 0 || param_array.length != 2) {
        throw new IllegalArgumentException("参数格式错误");
      }
      JDBCType param_type;
      String type_array[] = StringUtils.split(param_array[1], "=");
      if (type_array == null || type_array.length != 2 || StringUtils.isBlank(param_array[0])) {
        throw new IllegalArgumentException("参数类型定义错误");
      }
      if (!StringUtils.equalsIgnoreCase("jdbcType", StringUtils.trim(type_array[0]))) {
        throw new IllegalArgumentException("参数类型格式错误");
      }
      try {
        param_type = JDBCType.valueOf(type_array[1]);
      } catch(IllegalArgumentException e) {
        throw new IllegalArgumentException(String.format("#{%s}类型错误", param_array[0]), e);
      }

      paramters.put(
        StringUtils.trim(param_array[0]),
        param_type
      );
      index_start = param_end + 1;
      param_start = sql.indexOf(Constants.SQL_PARAM_PREFIX, index_start);
    }

    return paramters;
  }

  public static MappedStatement createMappedStatement(
    String mapped_id,
    Configuration configuration,
    SqlCommandType command_type,
    String sql,
    ResultDefinition result
  ) {
    RawSqlSource sql_source = new RawSqlSource(
      configuration, sql, null
    );

    List<ResultMapping> result_lst = new ArrayList<>();

    if (result != null && result.hasColumn()) {
      for (String column_name : result.getColumns().keySet()) {
        ResultDefinition.ResultColumn result_column = result.getColumns().get(column_name);
        if (Objects.equals(ResultDefinition.ResultType.raw, result_column.getType())) {
          continue;
        }
        ResultMapping.Builder mapping_builder = new ResultMapping.Builder(
          configuration, result_column.getName()
        );
        mapping_builder.column(result_column.getName());
        if (Objects.equals(result_column.getType(), ResultDefinition.ResultType.map)) {
          mapping_builder.javaType(Map.class);
          mapping_builder.typeHandler(new com.snz1.jdbc.rest.dao.MapTypeHandler());
        } else if (Objects.equals(result_column.getType(), ResultDefinition.ResultType.list)) {
          mapping_builder.javaType(List.class);
          mapping_builder.typeHandler(new com.snz1.jdbc.rest.dao.ListTypeHandler());
        } else if (Objects.equals(result_column.getType(), ResultDefinition.ResultType.base64)) {
          mapping_builder.javaType(String.class);
          mapping_builder.typeHandler(new com.snz1.jdbc.rest.dao.Base64TypeHandler());
        }
        result_lst.add(mapping_builder.build());
      }
    }

    ResultMap result_map = new ResultMap.Builder(
      configuration, mapped_id,
      Map.class, result_lst,
      true
    ).build();

    MappedStatement ms = new MappedStatement.Builder(
      configuration, mapped_id, sql_source, command_type
    ).resultMaps(Arrays.asList(result_map)).build();
    return ms;
  }

}
