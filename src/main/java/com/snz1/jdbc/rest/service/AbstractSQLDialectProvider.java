package com.snz1.jdbc.rest.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import org.apache.ibatis.jdbc.SQL;

import com.snz1.jdbc.rest.data.JdbcQuery;
import com.snz1.jdbc.rest.data.JdbcQueryRequest;
import com.snz1.jdbc.rest.data.ManipulationRequest;
import com.snz1.jdbc.rest.data.TableColumn;
import com.snz1.jdbc.rest.data.TableQueryRequest;
import com.snz1.jdbc.rest.data.WhereCloumn;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractSQLDialectProvider implements SQLDialectProvider {

  // 获取查询的合计
  @Override
  public JdbcQuery prepareQueryCount(TableQueryRequest table_query) {
    long start_time = System.currentTimeMillis();
    JdbcQuery base_query = null;
    try {
      base_query = this.createQueryRequestBaseSQL(table_query, true);
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
  protected JdbcQuery createQueryRequestBaseSQL(TableQueryRequest table_query, boolean docount) {
    long start_time = System.currentTimeMillis();
    SQL sql = new SQL();
    List<Object> parameters = new LinkedList<Object>();
    try {
      sql.FROM(table_query.getTable_name());

      if (table_query.getSelect().hasCount() || docount) {
        if (!docount && table_query.hasGroup_by()) {
          table_query.getGroup_by().forEach(g -> {
            sql.SELECT(g.getColumn());
          });
        }
        if (table_query.getSelect().hasCount()) {
          sql.SELECT(String.format("count(%s)", table_query.getSelect().getCount()));
        } else {
          sql.SELECT("count(*)");
        }
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

      if (!docount && table_query.hasGroup_by()) {
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
        for (WhereCloumn w : table_query.getWhere()) {
          if (where_append) {
            sql.AND();
          } else {
            where_append = true;
          }
          sql.WHERE(w.toWhereSQL());
          w.buildParameters(parameters);
        };
      }

      if (!docount && table_query.hasOrder_by()) {
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

  // 插入数据
  protected String createInsertRequestBaseSQL(ManipulationRequest insert_request) {
    long start_time = System.currentTimeMillis();
    SQL sql = new SQL();
    try {
      sql.INSERT_INTO(insert_request.getTable_name());
      insert_request.getColumns().forEach(v -> {
        if (v.getRead_only() != null && v.getRead_only()) return;
        if (v.getAuto_increment() != null && v.getAuto_increment()) return;
        if (v.getWritable() != null && v.getWritable()) {
          sql.VALUES(v.getName(), "?");
        }
      });
      return sql.toString();
    } finally {
      if (log.isDebugEnabled()) {
        log.debug(
          "构建数据插入SQL耗时{}毫秒, SQL:\n{}",
          (System.currentTimeMillis() - start_time),
          sql.toString()
        );
      }
    }
  }

  // 更新数据
  protected String createUpdateRequestBaseSQL(ManipulationRequest update_request) {
    long start_time = System.currentTimeMillis();
    SQL sql = new SQL();
    try {
      sql.UPDATE(update_request.getTable_name());
      if (update_request.hasWhere() || update_request.isPatch_update()) { // 条件更新或补丁更新
        update_request.getInput_map().forEach((k, p) -> {
          TableColumn v = update_request.findColumn(k);
          if (v.getRead_only() != null && v.getRead_only()) return;
          if (v.getAuto_increment() != null && v.getAuto_increment()) return;
          if (v.getWritable() != null && v.getWritable()) {
            sql.SET(String.format("%s = ?", v.getName()));
          }
        });
      } else { // 主键更新
        update_request.getColumns().forEach(v -> {
          if (v.getRead_only() != null && v.getRead_only()) return;
          if (v.getAuto_increment() != null && v.getAuto_increment()) return;
          if (update_request.testRow_key(v.getName())) return;
          if (v.getWritable() != null && v.getWritable()) {
            sql.SET(String.format("%s = ?", v.getName()));
          }
        });
      }
      if (update_request.hasWhere()) { // 有查询条件的更新
        boolean append = false;
        for (int i = 0; i < update_request.getWhere().size(); i++) {
          if (append) {
            sql.AND();
          } else {
            append = true;
          }
          sql.WHERE(update_request.getWhere().get(i).toWhereSQL());
        }
      } else { // 主键更新
        Object rowkey = update_request.getRow_key();
        if (update_request.testComposite_key()) {
          List<?> row_keys = (List<?>)rowkey;
          boolean append = false;
          for (int i = 0; i < row_keys.size(); i++) {
            if (append) {
              sql.AND();
            } else {
              append = true;
            }
            sql.WHERE(String.format("%s = ?", row_keys.get(i)));
          }
        } else {
          sql.WHERE(String.format("%s = ?", rowkey));
        }
      }
      return sql.toString();
    } finally {
      if (log.isDebugEnabled()) {
        log.debug(
          "构建数据更新SQL耗时{}毫秒, SQL:\n{}",
          (System.currentTimeMillis() - start_time),
          sql.toString()
        );
      }
    }
  }

  // 删除数据
  protected String createDeleteRequestBaseSQL(ManipulationRequest update_request) {
    long start_time = System.currentTimeMillis();
    SQL sql = new SQL();
    try {
      sql.DELETE_FROM(update_request.getTable_name());
      if (update_request.hasWhere()) { // 有查询条件的更新
        boolean append = false;
        for (int i = 0; i < update_request.getWhere().size(); i++) {
          if (append) {
            sql.AND();
          } else {
            append = true;
          }
          sql.WHERE(update_request.getWhere().get(i).toWhereSQL());
        }
      } else { // 主键删除
        Object rowkey = update_request.getRow_key();
        if (update_request.testComposite_key()) {
          List<?> row_keys = (List<?>)rowkey;
          boolean append = false;
          for (int i = 0; i < row_keys.size(); i++) {
            if (append) {
              sql.AND();
            } else {
              append = true;
            }
            sql.WHERE(String.format("%s = ?", row_keys.get(i)));
          }
        } else {
          sql.WHERE(String.format("%s = ?", rowkey));
        }
      }
      return sql.toString();
    } finally {
      if (log.isDebugEnabled()) {
        log.debug(
          "构建数据删除SQL耗时{}毫秒, SQL:\n{}",
          (System.currentTimeMillis() - start_time),
          sql.toString()
        );
      }
    }
  }

  // 准备更新数据
  public PreparedStatement prepareDataUpdate(
    Connection conn,
    ManipulationRequest update_request
  ) throws SQLException {
    StringBuffer sqlbuf = new StringBuffer(this.createUpdateRequestBaseSQL(update_request));
    return conn.prepareStatement(sqlbuf.toString());
  }

  // 准备删除数据
  public PreparedStatement prepareDataDelete(
    Connection conn,
    ManipulationRequest delete_request
  ) throws SQLException {
    StringBuffer sqlbuf = new StringBuffer(this.createDeleteRequestBaseSQL(delete_request));
    return conn.prepareStatement(sqlbuf.toString());
  }

}
