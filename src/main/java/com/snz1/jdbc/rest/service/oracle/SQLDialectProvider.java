package com.snz1.jdbc.rest.service.oracle;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.ibatis.jdbc.SQL;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.stereotype.Component;

import com.snz1.jdbc.rest.data.JdbcQueryStatement;
import com.snz1.jdbc.rest.data.ManipulationRequest;
import com.snz1.jdbc.rest.data.TableDefinition;
import com.snz1.jdbc.rest.data.WhereCloumn;
import com.snz1.jdbc.rest.data.ConditionOperation;
import com.snz1.jdbc.rest.data.JdbcQueryRequest;
import com.snz1.jdbc.rest.service.AbstractSQLDialectProvider;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component("oracleSQLDialectProvider")
@ConditionalOnClass(oracle.jdbc.driver.OracleDriver.class)
public class SQLDialectProvider extends AbstractSQLDialectProvider {
    
  public static final String NAME = "oracle";

  @Override
  public String getId() {
    return NAME;
  }

  public static void setupDatabaseEnvironment(ConfigurableEnvironment environment) {
    Map<String, Object> database_properties = new HashMap<>();
    database_properties.put("DB_VALIDATION_QUERY", "SELECT 1+1 FROM DUAL");
		environment.getPropertySources().addLast(new MapPropertySource("jdbcrest", database_properties));
  }

  public static void createDatabaseIfNotExisted(
    Connection conn, String databaseName, String databaseUsername, String databasePassword
  ) {
    if (log.isDebugEnabled()) {
      log.debug("无法动态创建数据库");
    }
    throw new IllegalStateException("无法动态创建数据库");
  }

  // 查询对象
  protected JdbcQueryStatement createQueryRequestBaseSQL(JdbcQueryRequest table_query, Long offset, Long limit, boolean docount) {
    long start_time = System.currentTimeMillis();
    SQL sql = new SQL();
    List<Object> parameters = new LinkedList<Object>();
    try {
      sql.FROM(table_query.getFullTableName());

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
        sql.SELECT("ROWNUM AS ROWNUM__");
        if (table_query.getSelect().isDistinct()) {
          sql.SELECT_DISTINCT(table_query.getFullTableName() + ".*");
        } else {
          sql.SELECT(table_query.getFullTableName() + ".*");
        }
      } else {
        sql.SELECT("ROWNUM AS ROWNUM__");
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
            j.getFullTableName(),
            j.getFullTableName(),
            j.getJoin_column(),
            table_query.getFullTableName(),
            j.getOuter_column()
          ));
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
          sql.WHERE(w.toWhereSQL(this.typeConverterFactory));
          w.buildParameters(parameters, this.typeConverterFactory);
        };
      }

      TableDefinition table_definition = table_query.getDefinition();
      if (table_definition != null && table_definition.hasOwner_id_column()) {
        if (where_append) {
          sql.AND();
        } else {
          where_append = true;
        }
        sql.WHERE(String.format("%s.%s = ?", table_definition.resolveName(), table_definition.getOwner_id_column().getName()));
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
          sql.WHERE(w.toWhereSQL(this.typeConverterFactory));
          w.buildParameters(parameters, this.typeConverterFactory);
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
        sql.WHERE(w.toWhereSQL(this.typeConverterFactory));
        w.buildParameters(parameters, this.typeConverterFactory);
      }

      // 添加分页
      if (where_append) {
        sql.AND();
      } else {
        where_append = true;
      }
      sql.WHERE(String.format("ROWNUM <= %d", (offset + limit)));

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

    return new JdbcQueryStatement(sql.toString(), parameters);
  }

  @Override
  public PreparedStatement preparePageSelect(Connection conn, JdbcQueryRequest table_query) throws SQLException {
    JdbcQueryStatement base_query = this.createQueryRequestBaseSQL(
      table_query, table_query.getResult().getOffset(),
      table_query.getResult().getLimit(), false);

    StringBuffer sqlbuf = new StringBuffer();
    if (table_query.getSelect().hasCount()) {
      sqlbuf.append(base_query.getSql());
    } else {
      sqlbuf.append("SELECT * FROM (")
            .append(base_query.getSql())
            .append(") WHERE ROWNUM__ > ")
            .append(table_query.getResult().getOffset());
    }
    if (log.isDebugEnabled()) {
      log.info("构建Oracle分页查询语句:\n{}", sqlbuf.toString());
    }

    PreparedStatement ps = conn.prepareStatement(sqlbuf.toString());
    if (base_query.hasParameter()) {
      for (int i = 0; i < base_query.getParameters().size(); i++) {
        Object param = base_query.getParameters().get(i);
        ps.setObject(i + 1, param);
      };
    }
    return ps;
  }

  public PreparedStatement prepareDataInsert(Connection conn, ManipulationRequest insert_request) throws SQLException {
    StringBuffer sqlbuf = new StringBuffer(this.createInsertRequestBaseSQL(insert_request));
    if (insert_request.hasPrimary_key()) {
      // 添加冲突处理, TODO: 未验证
      StringBuffer ignore_sql = new StringBuffer(" /*+ IGNORE_ROW_ON_DUPKEY_INDEX(");
      ignore_sql.append(insert_request.getFullTableName())
        .append("(")
        .append(insert_request.getPrimary_key())
        .append(")) */");
      sqlbuf.insert(6, ignore_sql.toString()); // 在insert后插入ignore
    }
    return conn.prepareStatement(sqlbuf.toString());
  }

}
