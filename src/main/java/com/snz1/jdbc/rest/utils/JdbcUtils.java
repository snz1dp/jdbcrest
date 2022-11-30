package com.snz1.jdbc.rest.utils;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.lang3.StringUtils;
import org.postgresql.jdbc.PgArray;

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

}
