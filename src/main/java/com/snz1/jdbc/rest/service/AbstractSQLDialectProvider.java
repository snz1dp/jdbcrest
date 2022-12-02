package com.snz1.jdbc.rest.service;

import java.util.LinkedList;
import java.util.List;

import org.apache.ibatis.jdbc.SQL;

import com.snz1.jdbc.rest.data.JdbcQuery;
import com.snz1.jdbc.rest.data.JdbcQueryRequest;
import com.snz1.jdbc.rest.data.TableQueryRequest;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractSQLDialectProvider implements SQLDialectProvider {

  // 获取查询的合计
  @Override
  public JdbcQuery prepareQueryCount(TableQueryRequest table_query) {
    long start_time = System.currentTimeMillis();
    JdbcQuery base_query = null;
    try {
      base_query = this.createQueryRequestBaseSQL(table_query);
      StringBuffer count_buf = new StringBuffer("select count(*) from (");
      count_buf.append(base_query.getSql())
              .append(") AS a");
      base_query.setSql(count_buf.toString());
      return base_query;
    } finally {
      if (log.isDebugEnabled()) {
        log.debug(
          "构建统计查询SQL耗时{}毫秒, SQL:\n{}",
          (System.currentTimeMillis() - start_time),
          base_query != null ? base_query.getSql() : "构建失败"
        );
      }
    }
  }

  // 查询对象
  protected JdbcQuery createQueryRequestBaseSQL(TableQueryRequest table_query) {
    long start_time = System.currentTimeMillis();
    SQL sql = new SQL();
    List<Object> parameters = new LinkedList<Object>();
    try {
      sql.FROM(table_query.getTable_name());

      if (table_query.getSelect().hasCount()) {
        if (table_query.hasGroup_by()) {
          table_query.getGroup_by().forEach(g -> {
            sql.SELECT(g.getColumn());
          });
        }
        sql.SELECT(String.format("count(%s)", table_query.getSelect().getCount()));
      } else if (!table_query.getSelect().hasColumns()) {
        if (table_query.getSelect().isDistinct()) {
          sql.SELECT_DISTINCT("*");
        } else {
          sql.SELECT("*");
        }
      } else {
        table_query.getSelect().getColumns().forEach(c -> {
          if (c.hasFunction()) {
            if (c.hasAs()) {
              if (table_query.getSelect().isDistinct()) {
                sql.SELECT_DISTINCT(String.format("%s(%s) AS %s", c.getFunction(), c.getColumn(), c.getAs()));
              } else {
                sql.SELECT(String.format("%s(%s) AS %s", c.getFunction(), c.getColumn(), c.getAs()));
              }
            } else if (table_query.getSelect().isDistinct()) {
              sql.SELECT_DISTINCT(String.format("%s(%s)", c.getFunction(), c.getColumn()));
            } else {
              sql.SELECT(String.format("%s(%s)", c.getFunction(), c.getColumn()));
            }
          } else if (c.hasAs()) {
            if (table_query.getSelect().isDistinct()) {
              sql.SELECT_DISTINCT(String.format("%s AS %s", c.getColumn(), c.getAs()));
            } else {
              sql.SELECT(String.format("%s AS %s", c.getColumn(), c.getAs()));
            }
          } else if (table_query.getSelect().isDistinct()) {
            sql.SELECT_DISTINCT(c.getColumn());
          } else {
            sql.SELECT(c.getColumn());
          }
        });
      }

      if (table_query.hasJoin()) {
        table_query.getJoin().forEach(j -> {
          sql.LEFT_OUTER_JOIN(String.format(
            "%s on %s.%s = %s.%s",
            j.getTable_name(),
            j.getTable_name(),
            j.getJoin_column(),
            table_query.getTable_name(),
            j.getOuter_column()
          ));
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
    } finally {
      if (log.isDebugEnabled()) {
        log.debug(
          "构建数据表查询SQL耗时{}毫秒, SQL:\n{}",
          (System.currentTimeMillis() - start_time),
          sql.toString()
        );
      }
    }

    return new JdbcQuery(sql.toString(), parameters);
  }

}
