package com.snz1.jdbc.rest.service;

import java.util.LinkedList;
import java.util.List;

import org.apache.ibatis.jdbc.SQL;

import com.snz1.jdbc.rest.data.JdbcQuery;
import com.snz1.jdbc.rest.data.JdbcQueryRequest;
import com.snz1.jdbc.rest.data.TableQueryRequest;

public abstract class AbstractSQLDialectProvider implements SQLDialectProvider {

  // 获取查询的合计
  @Override
  public JdbcQuery prepareQueryCount(TableQueryRequest table_query) {
    JdbcQuery base_query = this.createQueryRequestBaseSQL(table_query);
    StringBuffer count_buf = new StringBuffer("select count(*) from (");
    count_buf.append(base_query.getSql())
            .append(") AS a");
    base_query.setSql(count_buf.toString());
    return base_query;
  }

  // 查询对象
  protected JdbcQuery createQueryRequestBaseSQL(TableQueryRequest table_query) {
    SQL sql = new SQL();
    List<Object> parameters = new LinkedList<Object>();
    sql.FROM(table_query.getTable_name());

    if (!table_query.getSelect().hasColumns()) {
      sql.SELECT("*");
    } else {
      table_query.getSelect().getColumns().forEach(c -> {
        if (c.hasFunction()) {
          if (c.hasAs()) {
            sql.SELECT(String.format("%s(%s) AS %s", c.getFunction(), c.getColumn(), c.getAs()));
          } else {
            sql.SELECT(String.format("%s(%s)", c.getFunction(), c.getColumn()));
          }
        } else if (c.hasAs()) {
          sql.SELECT(String.format("%s AS %s", c.getColumn(), c.getAs()));
        } else {
          sql.SELECT(c.getColumn());
        }
      });
    }

    if (table_query.hasGroup_by()) {
      table_query.getGroup_by().forEach(g -> {
        sql.GROUP_BY(g.getColumn());
        if (g.hasHaving()) {
          sql.HAVING(g.getHaving().toHavingSQL());
          g.getHaving().buildParameters(parameters);
        }
      });
    }

    if (table_query.hasWhere()) {
      boolean where_append = false;
      for (JdbcQueryRequest.WhereCloumn w : table_query.getWhere()) {
        if (where_append) {
          sql.AND();
        } else {
          where_append = true;
        }
        sql.WHERE(w.toWhereSQL());
        w.buildParameters(parameters);
      };
    }

    if (table_query.hasOrder_by()) {
      boolean order_append = false;
      for (JdbcQueryRequest.OrderBy o : table_query.getOrder_by()) {
        if (order_append) {
          sql.AND();
        } else {
          order_append = true;
        }
        sql.ORDER_BY(o.toOrderSQL());
      };
    }

    return new JdbcQuery(sql.toString(), parameters);
  }

}
