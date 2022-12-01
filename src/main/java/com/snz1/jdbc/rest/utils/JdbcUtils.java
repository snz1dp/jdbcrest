package com.snz1.jdbc.rest.utils;

import java.math.BigInteger;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

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
    if (list.get(0) instanceof String) {
      if (type == JDBCType.DATE || type == JDBCType.TIMESTAMP || type == JDBCType.TIMESTAMP_WITH_TIMEZONE) {
        List<Date> datelist = new LinkedList<>();
        list.forEach(o -> {
          datelist.add(JsonUtils.fromJson((String)o, Date.class));
        });
        return datelist.toArray(new Date[0]);
      } else if (type == JDBCType.TIME) {
        List<Time> datelist = new LinkedList<>();
        list.forEach(o -> {
          datelist.add(JsonUtils.fromJson((String)o, Time.class));
        });
        return datelist.toArray(new Time[0]);
      } else if (type == JDBCType.BIGINT) {
        List<BigInteger> datelist = new LinkedList<>();
        list.forEach(o -> {
          datelist.add(BigInteger.valueOf(Long.parseLong((String)o)));
        });
        return datelist.toArray(new BigInteger[0]);
      } else if (type == JDBCType.INTEGER || type == JDBCType.SMALLINT || type == JDBCType.TINYINT || type == JDBCType.BIT) {
        List<Integer> datelist = new LinkedList<>();
        list.forEach(o -> {
          datelist.add(Integer.parseInt((String)o));
        });
        return datelist.toArray(new Integer[0]);
      } else if (type == JDBCType.FLOAT || type == JDBCType.DOUBLE || type == JDBCType.DECIMAL) {
        List<Double> datelist = new LinkedList<>();
        list.forEach(o -> {
          datelist.add(Double.parseDouble((String)o));
        });
        return datelist.toArray(new Double[0]);
      }
    } else if (list.get(0) instanceof Integer) {
      return list.toArray(new Integer[0]);
    } else if (list.get(0) instanceof Boolean) {
      return list.toArray(new Boolean[0]);
    } else if (list.get(0) instanceof Double) {
      if (type == JDBCType.BIGINT) {
        List<BigInteger> datelist = new LinkedList<>();
        list.forEach(o -> {
          datelist.add(BigInteger.valueOf(((Double)o).longValue()));
        });
        return datelist.toArray(new BigInteger[0]);
      } if (type == JDBCType.INTEGER || type == JDBCType.SMALLINT || type == JDBCType.TINYINT || type == JDBCType.BIT) {
        List<Integer> datelist = new LinkedList<>();
        list.forEach(o -> {
          datelist.add(Integer.valueOf((int)((Double)o).longValue()));
        });
        return datelist.toArray(new Integer[0]);
      } else {
        return list.toArray(new Double[0]);
      }
    }
    return null;
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

  public static Object toArray(String val, JDBCType type) {
    if (StringUtils.startsWith(val, "[")) {
      return jsonToArray(val, type);
    } else {
      return splitToArray(val, type);
    }
  }

}
