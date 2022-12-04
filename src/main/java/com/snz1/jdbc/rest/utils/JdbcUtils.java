package com.snz1.jdbc.rest.utils;

import java.lang.reflect.Array;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.postgresql.jdbc.PgArray;

import com.snz1.utils.JsonUtils;

public abstract class JdbcUtils extends org.springframework.jdbc.support.JdbcUtils {
  
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
        return input;
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

}
