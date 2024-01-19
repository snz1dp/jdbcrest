package com.snz1.jdbc.rest.provider;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.lang.Override;

import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.jdbc.SQL;

import com.snz1.jdbc.rest.data.JdbcQueryStatement;
import com.snz1.jdbc.rest.data.ConditionOperation;
import com.snz1.jdbc.rest.data.JdbcQueryRequest;
import com.snz1.jdbc.rest.data.ManipulationRequest;
import com.snz1.jdbc.rest.data.TableColumn;
import com.snz1.jdbc.rest.data.TableDefinition;
import com.snz1.jdbc.rest.data.WhereCloumn;
import com.snz1.jdbc.rest.service.JdbcTypeConverterFactory;
import com.snz1.jdbc.rest.service.LoggedUserContext;
import com.snz1.jdbc.rest.service.UserRoleVerifier;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractSQLDialectProvider implements SQLDialectProvider {

  protected LoggedUserContext loggedUserContext;

  protected JdbcTypeConverterFactory typeConverterFactory;

  protected UserRoleVerifier userRoleVerifier;

  public void setLoggedUserContext(LoggedUserContext loggedUserContext) {
    this.loggedUserContext = loggedUserContext;
  }

  public void setTypeConverterFactory(JdbcTypeConverterFactory typeConverterFactory) {
    this.typeConverterFactory = typeConverterFactory;
  }

  public JdbcTypeConverterFactory getTypeConverterFactory() {
    return typeConverterFactory;
  }

  public void setUserRoleVerifier(UserRoleVerifier userRoleVerifier) {
    this.userRoleVerifier = userRoleVerifier;
  }

  @Override
  public boolean checkTableExisted() {
    return true;
  }

  @Override
  public boolean supportSchemas() {
    return true;
  }

  @Override
  public boolean supportCountAnyColumns() {
    return true;
  }

  // 准备数据查询
  @Override
  public JdbcQueryStatement prepareQuerySelect(JdbcQueryRequest table_query) {
    long start_time = System.currentTimeMillis();
    JdbcQueryStatement base_query = null;
    try {
      base_query = this.createQueryRequestBaseSQL(table_query, false);
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


  // 获取查询的合计
  @Override
  @Deprecated
  public JdbcQueryStatement prepareQueryCount(JdbcQueryRequest table_query) {
    long start_time = System.currentTimeMillis();
    JdbcQueryStatement base_query = null;
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

  protected String buildQueryRequestBaseSQL(JdbcQueryRequest table_query, boolean docount, List<Object> parameters) {
    long start_time = System.currentTimeMillis();
    SQL sql = new SQL();
    try {
      sql.FROM(table_query.getFullTableName());

      if (table_query.getSelect().hasCount() || docount) {
        if (!docount && table_query.hasGroup_by()) {
          table_query.getGroup_by().forEach(g -> {
            sql.SELECT(g.getColumn());
          });
        }
        if (table_query.getSelect().hasCount()) {
          if (StringUtils.equals(table_query.getSelect().getCount(), "*") && this.supportCountAnyColumns()) {
            sql.SELECT(String.format("count(\"%s\")", table_query.getSelect().getCount()));
          } else {
            sql.SELECT(String.format("count(\"%s\")", table_query.getTable_meta().getColumns().get(0).getName()));
          }
        } else if (this.supportCountAnyColumns()) {
          sql.SELECT("count(*)");
        } else {
          sql.SELECT(String.format("count(%s)", table_query.getTable_meta().getColumns().get(0).getName()));
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
                sql.SELECT_DISTINCT(String.format("%s(\"%s\") AS %s", c.getFunction(), c.getColumn(), c.getAs()));
              } else {
                sql.SELECT(String.format("%s(\"%s\") AS %s", c.getFunction(), c.getColumn(), c.getAs()));
              }
            } else if (table_query.getSelect().isDistinct()) {
              sql.SELECT_DISTINCT(String.format("%s(\"%s\")", c.getFunction(), c.getColumn()));
            } else {
              sql.SELECT(String.format("%s(\"%s\")", c.getFunction(), c.getColumn()));
            }
          } else if (c.hasAs()) {
            if (table_query.getSelect().isDistinct()) {
              sql.SELECT_DISTINCT(String.format("\"%s\" AS %s", c.getColumn(), c.getAs()));
            } else {
              sql.SELECT(String.format("\"%s\" AS %s", c.getColumn(), c.getAs()));
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
          if (StringUtils.isBlank(j.getJoin_type())) {
            sql.JOIN(String.format(
              "%s on %s.\"%s\" = %s.\"%s\"",
              j.getFullTableName(),
              j.getFullTableName(),
              j.getJoin_column(),
              table_query.getFullTableName(),
              j.getOuter_column()
            ));
          } else if (StringUtils.equalsIgnoreCase(j.getJoin_type(), "inner")) {
            sql.INNER_JOIN(String.format(
              "%s on %s.\"%s\" = %s.\"%s\"",
              j.getFullTableName(),
              j.getFullTableName(),
              j.getJoin_column(),
              table_query.getFullTableName(),
              j.getOuter_column()
            ));
          } else if (StringUtils.equalsIgnoreCase(j.getJoin_type(), "outer")) {
            sql.OUTER_JOIN(String.format(
              "%s on %s.\"%s\" = %s.\"%s\"",
              j.getFullTableName(),
              j.getFullTableName(),
              j.getJoin_column(),
              table_query.getFullTableName(),
              j.getOuter_column()
            ));
          } else if (StringUtils.equalsIgnoreCase(j.getJoin_type(), "right outer")) {
            sql.RIGHT_OUTER_JOIN(String.format(
              "%s on %s.\"%s\" = %s.\"%s\"",
              j.getFullTableName(),
              j.getFullTableName(),
              j.getJoin_column(),
              table_query.getFullTableName(),
              j.getOuter_column()
            ));
          } else if (StringUtils.equalsIgnoreCase(j.getJoin_type(), "left outer")) {
            sql.LEFT_OUTER_JOIN(String.format(
              "%s on %s.\"%s\" = %s.\"%s\"",
              j.getFullTableName(),
              j.getFullTableName(),
              j.getJoin_column(),
              table_query.getFullTableName(),
              j.getOuter_column()
            ));
          }
        });
      }

      if (!docount && table_query.hasGroup_by()) {
        table_query.getGroup_by().forEach(g -> {
          sql.GROUP_BY(g.getColumn());
          if (g.hasHaving()) {
            sql.HAVING(g.getHaving().toHavingSQL(this.typeConverterFactory));
            g.getHaving().buildParameters(parameters, this.typeConverterFactory);
          }
        });
      }

      boolean where_append = false;
      if (table_query.hasWhere()) {
        for (WhereCloumn w : table_query.getWhere()) {
          if (where_append) {
            sql.AND();
          } else {
            where_append = true;
          }
          JdbcQueryStatement sts = w.buildSQLStatement(this);
          sql.WHERE(sts.getSql());
          parameters.addAll(sts.getParameters());
        };
      }

      TableDefinition table_definition = table_query.getDefinition();
      if (table_definition != null && table_definition.hasOwner_id_column()) {
        if (where_append) {
          sql.AND();
        } else {
          where_append = true;
        }
        sql.WHERE(String.format("\"%s\".\"%s\" = ?", table_definition.resolveName(), table_definition.getOwner_id_column().getName()));
        if (loggedUserContext.isUserLogged()) { 
          parameters.add(loggedUserContext.getLoggedIdByType(table_definition.getOwner_id_column().getIdtype()));
        } else {
          parameters.add(null);
        }
      }

      if (table_definition != null && table_definition.hasDefault_where()) {
        List<WhereCloumn> append_wehre = table_definition.copyDefault_where();
        for (WhereCloumn w : append_wehre) {
          if (where_append) {
            sql.AND();
          } else {
            where_append = true;
          }
          JdbcQueryStatement sts = w.buildSQLStatement(this);
          sql.WHERE(sts.getSql());
          parameters.addAll(sts.getParameters());
        };
      }

      if (table_definition != null && table_definition.hasOwner_app_column() && loggedUserContext.isUserLogged() && (
        !table_definition.hasAll_data_role() || !userRoleVerifier.isUserInAnyRole(
          loggedUserContext.getLoggedUser(), table_definition.getAll_data_role()) 
      )) {
        WhereCloumn w = WhereCloumn.of(table_definition.getOwner_app_column());
        w.setOr(true);
        for (String appcode : userRoleVerifier.getUserOwnerAppcodes(loggedUserContext.getLoggedUser())) {
          w.addCondition(table_definition.getOwner_app_column(), ConditionOperation.$eq, appcode);
        }
        JdbcQueryStatement sts = w.buildSQLStatement(this);
        sql.WHERE(sts.getSql());
        parameters.addAll(sts.getParameters());
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
    return sql.toString();
  }

  // 查询对象
  protected JdbcQueryStatement createQueryRequestBaseSQL(JdbcQueryRequest table_query, boolean docount) {
    List<Object> parameters = new LinkedList<Object>();
    String sql = this.buildQueryRequestBaseSQL(table_query, docount, parameters);
    return new JdbcQueryStatement(sql, parameters);
  }

  // 插入数据
  @SuppressWarnings("unchecked")
  protected String createInsertRequestBaseSQL(ManipulationRequest insert_request) {
    long start_time = System.currentTimeMillis();
    SQL sql = new SQL();
    try {
      sql.INSERT_INTO(insert_request.getFullTableName());
      if (insert_request.hasColumns()) {
        insert_request.getColumns().forEach(v -> {
          if (v.getAuto_increment() != null && v.getAuto_increment()) return;
          sql.VALUES("\"" + v.getName() + "\"", "?");
        });
      } else {
        Map<String, Object> input_line0 = null;
        if (insert_request.getInput_data() instanceof List) {
          input_line0 = ((List<Map<String, Object>>)insert_request.getInput_data()).get(0);
        } else {
          input_line0 = (Map<String, Object>)insert_request.getInput_data();
        }
        for (String key : input_line0.keySet()){
          sql.VALUES("\"" + key + "\"", "?");
        }
      }
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
  protected JdbcQueryStatement createUpdateRequestBaseSQL(ManipulationRequest update_request) {
    long start_time = System.currentTimeMillis();
    SQL sql = new SQL();
    List<Object> parameters = new LinkedList<Object>();
    try {
      TableDefinition table_definition = update_request.getDefinition();
      sql.UPDATE(update_request.getFullTableName());
      if (update_request.isPatch_update()) { // 条件更新或补丁更新
        for (TableColumn v : update_request.getColumns()) {
          String k = v.getName();
          Map<String, Object> input_map = update_request.getInput_map();
          if (!input_map.containsKey(k)) continue;
          if (v.getAuto_increment() != null && v.getAuto_increment()) continue;
          if (table_definition != null && table_definition.inColumn(k)) continue;
          sql.SET(String.format("\"%s\" = ?", k));
        }
      } else { // 主键更新
        update_request.getColumns().forEach(v -> {
          if (v.getAuto_increment() != null && v.getAuto_increment()) return;
          if (update_request.testRow_key(v.getName())) return;
          if (table_definition != null && table_definition.inColumn(v.getName())) return;
          sql.SET(String.format("\"%s\" = ?", v.getName()));
        });
      }

      if (table_definition != null) {
        if (table_definition.hasUpdated_time_column()) {
          sql.SET(String.format("\"%s\" = ?", table_definition.getUpdated_time_column()));
        }

        if (table_definition.hasMender_id_column()) {
          sql.SET(String.format("\"%s\" = ?", table_definition.getMender_id_column().getName()));
        }

        if (table_definition.hasMender_name_column()) {
          sql.SET(String.format("\"%s\" = ?", table_definition.getMender_name_column()));
        }
      }

      boolean where_append = false;
      if (table_definition != null && table_definition.hasDefault_where()) {
        List<WhereCloumn> append_wehre = table_definition.copyDefault_where();
        for (WhereCloumn w : append_wehre) {
          if (where_append) {
            sql.AND();
          } else {
            where_append = true;
          }
          JdbcQueryStatement sts = w.buildSQLStatement(this);
          sql.WHERE(sts.getSql());
          parameters.addAll(sts.getParameters());
        };
      }

      if (update_request.hasWhere()) { // 有查询条件的更新
        for (int i = 0; i < update_request.getWhere().size(); i++) {
          if (where_append) {
            sql.AND();
          } else {
            where_append = true;
          }
          WhereCloumn w = update_request.getWhere().get(i);
          JdbcQueryStatement sts = w.buildSQLStatement(this);
          sql.WHERE(sts.getSql());
          parameters.addAll(sts.getParameters());
        }
      } else { // 主键更新
        Object rowkey = update_request.getRow_key();
        if (update_request.testComposite_key()) {
          List<?> row_keys = (List<?>)rowkey;
          for (int i = 0; i < row_keys.size(); i++) {
            if (where_append) {
              sql.AND();
            } else {
              where_append = true;
            }
            sql.WHERE(String.format("\"%s\" = ?", row_keys.get(i)));
          }
        } else {
          sql.WHERE(String.format("\"%s\" = ?", rowkey));
        }
      }

      if (table_definition != null && table_definition.hasOwner_id_column()) {
        if (where_append) {
          sql.AND();
        } else {
          where_append = true;
        }
        sql.WHERE(String.format("\"%s\".\"%s\" = ?", table_definition.resolveName(), table_definition.getOwner_id_column().getName()));
      }

      return new JdbcQueryStatement(sql.toString(), parameters);
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
  protected JdbcQueryStatement createDeleteRequestBaseSQL(ManipulationRequest update_request) {
    long start_time = System.currentTimeMillis();
    SQL sql = new SQL();
    List<Object> parameters = new LinkedList<Object>();
    try {
      sql.DELETE_FROM(update_request.getFullTableName());
      TableDefinition table_definition = update_request.getDefinition();
      boolean where_append = false;

      if (table_definition != null && table_definition.hasDefault_where()) {
        List<WhereCloumn> append_wehre = table_definition.copyDefault_where();
        for (WhereCloumn w : append_wehre) {
          if (where_append) {
            sql.AND();
          } else {
            where_append = true;
          }
          JdbcQueryStatement sts = w.buildSQLStatement(this);
          sql.WHERE(sts.getSql());
          parameters.addAll(sts.getParameters());
        };
      }

      if (update_request.hasWhere()) { // 有查询条件的更新
        for (int i = 0; i < update_request.getWhere().size(); i++) {
          if (where_append) {
            sql.AND();
          } else {
            where_append = true;
          }
          WhereCloumn w = update_request.getWhere().get(i);
          JdbcQueryStatement sts = w.buildSQLStatement(this);
          sql.WHERE(sts.getSql());
          parameters.addAll(sts.getParameters());
        }
      } else { // 主键删除
        Object rowkey = update_request.getRow_key();
        if (update_request.testComposite_key()) {
          List<?> row_keys = (List<?>)rowkey;
          for (int i = 0; i < row_keys.size(); i++) {
            if (where_append) {
              sql.AND();
            } else {
              where_append = true;
            }
            sql.WHERE(String.format("\"%s\" = ?", row_keys.get(i)));
          }
        } else {
          sql.WHERE(String.format("\"%s\" = ?", rowkey));
        }
      }

      if (table_definition != null && table_definition.hasOwner_id_column()) {
        if (where_append) {
          sql.AND();
        } else {
          where_append = true;
        }
        sql.WHERE(String.format("\"%s\".\"%s\" = ?", table_definition.resolveName(), table_definition.getOwner_id_column().getName()));
      }

      return new JdbcQueryStatement(sql.toString(), parameters);
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
  public JdbcQueryStatement prepareDataUpdate(
    ManipulationRequest update_request
  ) {
    return this.createUpdateRequestBaseSQL(update_request);
  }

  // 准备删除数据
  @Override
  public JdbcQueryStatement prepareDataDelete(
    ManipulationRequest delete_request
  ) {
    return this.createDeleteRequestBaseSQL(delete_request);
  }

  @Override
  public JdbcQueryStatement prepareDataInsert(ManipulationRequest insert_request) {
    StringBuffer sqlbuf = new StringBuffer(this.createInsertRequestBaseSQL(insert_request));
    return new JdbcQueryStatement(sqlbuf.toString(), null);
  }

}
