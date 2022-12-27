package com.snz1.jdbc.rest.service.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.springframework.dao.DataAccessException;
import org.springframework.lang.Nullable;

import com.snz1.jdbc.rest.data.ManipulationRequest;
import com.snz1.jdbc.rest.data.TableColumn;
import com.snz1.jdbc.rest.data.TableDefinition;
import com.snz1.jdbc.rest.data.WhereCloumn;
import com.snz1.jdbc.rest.service.JdbcTypeConverterFactory;
import com.snz1.jdbc.rest.service.LoggedUserContext;
import com.snz1.jdbc.rest.service.SQLDialectProvider;
import com.snz1.jdbc.rest.service.LoggedUserContext.UserInfo;
import com.snz1.jdbc.rest.utils.JdbcUtils;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

// 更新请求处理
@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class UpdateRequestHandler extends AbstractManipulationRequestHandler<int[]> {
  
  public UpdateRequestHandler(
    ManipulationRequest update_request, SQLDialectProvider sql_dialect_provider,
    JdbcTypeConverterFactory type_converter_factory, LoggedUserContext loggedUserContext
  ) {
    super(update_request, sql_dialect_provider, type_converter_factory, loggedUserContext);
  }

  private Map<String, Object> doBuildUpdateRequestInputData(Map<String, Object> input_data) {
    ManipulationRequest updateRequest = this.getRequest();
    TableDefinition definition = updateRequest.getDefinition();
    if (definition == null) {
      return input_data;
    }

    LoggedUserContext loggedUserContext = this.getLoggedUserContext();
    input_data = new LinkedHashMap<>(input_data);

    if (definition.hasUpdated_time_column()) {
      input_data.put(definition.getUpdated_time_column(), updateRequest.getRequest_time());
    }

    UserInfo logged_user = null;
    if (loggedUserContext.isUserLogged()) {
      logged_user = loggedUserContext.getLoginUserInfo();
    }

    if (definition.hasUpdated_time_column()) {
      input_data.put(definition.getUpdated_time_column(), updateRequest.getRequest_time());
    }

    if (definition.hasMender_id_column()) {
      TableDefinition.UserIdColumn userid = definition.getMender_id_column();
      if (logged_user != null) {
        input_data.put(userid.getName(), logged_user.getIdByType(userid.getIdtype()));
      } else {
        input_data.put(userid.getName(), null);
      }
    }

    if (definition.hasMender_name_column()) {
      if (logged_user != null) {
        input_data.put(definition.getMender_name_column(), logged_user.getDisplay_name());
      } else {
        input_data.put(definition.getMender_name_column(), null);
      }
    }

    if (definition.hasOwner_id_column()) {
      TableDefinition.UserIdColumn userid = definition.getOwner_id_column();
      if (logged_user != null) {
        input_data.put(userid.getName(), logged_user.getIdByType(userid.getIdtype()));
      } else {
        input_data.put(userid.getName(), null);
      }
    }

    return input_data;
  }

  @Override
  @Nullable
  public int[] doInConnection(Connection conn) throws SQLException, DataAccessException {
    ManipulationRequest updateRequest = this.getRequest();
    TableDefinition tdf = updateRequest.getDefinition();
    SQLDialectProvider sqlDialectProvider = this.getSqlDialectProvider();
    JdbcTypeConverterFactory converterFactory = this.getConverterFactory();

    PreparedStatement ps = sqlDialectProvider.prepareDataUpdate(conn, updateRequest);
    try {
      List<Map<String, Object>> input_datas = updateRequest.getInput_list();
      for (Map<String, Object> input_data : input_datas) {
        int i = 1;
        input_data = doBuildUpdateRequestInputData(input_data);
        if (updateRequest.hasWhere() || updateRequest.isPatch_update()) {
          for (TableColumn v : updateRequest.getColumns()) {
            if (!input_data.containsKey(v.getName())) continue;
            if (v.getRead_only() != null && v.getRead_only()) continue;
            if (v.getAuto_increment() != null && v.getAuto_increment()) continue;
            if (tdf != null && tdf.inColumn(v.getName())) continue;
            if (v.getWritable() != null && v.getWritable()) {
              if (log.isDebugEnabled()) {
                log.debug("设置更新参数: PI={}, COLUMN={}, VALUE={}", i, v.getName(), input_data.get(v.getName()));
              }
              ps.setObject(i, converterFactory.convertObject(input_data.get(v.getName()), v.getJdbc_type()));
              i = i + 1;
            }
          }
        } else {
          for (TableColumn v : updateRequest.getColumns()) {
            if (v.getRead_only() != null && v.getRead_only()) continue;
            if (v.getAuto_increment() != null && v.getAuto_increment()) continue;
            if (updateRequest.testRow_key(v.getName())) continue;
            if (tdf != null && tdf.inColumn(v.getName())) continue;
            if (v.getWritable() != null && v.getWritable()) {
              if (log.isDebugEnabled()) {
                log.debug("设置更新参数: PI={}, COLUMN={}, VALUE={}", i, v.getName(), input_data.get(v.getName()));
              }
              ps.setObject(i, converterFactory.convertObject(input_data.get(v.getName()), v.getJdbc_type()));
              i = i + 1;
            }
          }
        }

        if (tdf != null) {
          if (tdf.hasUpdated_time_column()) {
            TableColumn v = updateRequest.findColumn(tdf.getUpdated_time_column());
            if (log.isDebugEnabled()) {
              log.debug("设置上下文更新参数: PI={}, COLUMN={}, VALUE={}", i, v.getName(), input_data.get(v.getName()));
            }
            ps.setObject(i, converterFactory.convertObject(input_data.get(v.getName()), v.getJdbc_type()));
            i = i + 1;
          }
  
          if (tdf.hasMender_id_column()) {
            TableColumn v = updateRequest.findColumn(tdf.getMender_id_column().getName());
            if (log.isDebugEnabled()) {
              log.debug("设置上下文更新参数: PI={}, COLUMN={}, VALUE={}", i, v.getName(), input_data.get(v.getName()));
            }
            ps.setObject(i, converterFactory.convertObject(input_data.get(v.getName()), v.getJdbc_type()));
            i = i + 1;
          }
  
          if (tdf.hasMender_name_column()) {
            TableColumn v = updateRequest.findColumn(tdf.getMender_name_column());
            if (log.isDebugEnabled()) {
              log.debug("设置上下文更新参数: PI={}, COLUMN={}, VALUE={}", i, v.getName(), input_data.get(v.getName()));
            }
            ps.setObject(i, converterFactory.convertObject(input_data.get(v.getName()), v.getJdbc_type()));
            i = i + 1;
          }
        }

        if (updateRequest.hasWhere()) {
          List<Object> where_conditions = new LinkedList<>();
          for (WhereCloumn w : updateRequest.getWhere()) {
            w.buildParameters(where_conditions, converterFactory);
          }
          for (Object where_object : where_conditions) {
            ps.setObject(i, where_object);
            if (log.isDebugEnabled()) {
              log.debug("设置更新条件参数: PI={}, WHERE={}", i, where_object);
            }
            i = i + 1;
          }
        } else {
          Object keyvalue = updateRequest.getInput_key();
          Object rowkey = updateRequest.getRow_key();
          if (updateRequest.testComposite_key()) {
            List<?> row_keys = (List<?>)rowkey;
            List<?> key_values = (List<?>)keyvalue;
            for (int j = 0; j < row_keys.size(); j++) {
              String keyname = (String)row_keys.get(j);
              Object keyval = key_values.get(j);
              TableColumn v = updateRequest.findColumn(keyname);
              if (log.isDebugEnabled()) {
                log.debug("设置多主键条件参数: PI={}, COLUMN={}, VALUE={}", i, v.getName(), keyval);
              }
              ps.setObject(i, converterFactory.convertObject(
                keyval, v != null ? v.getJdbc_type() : null
              ));
              i = i + 1;
            }
          } else {
            TableColumn v = updateRequest.findColumn((String)rowkey);
            if (log.isDebugEnabled()) {
              log.debug("设置单主键条件参数: PI={}, COLUMN={}, VALUE={}", i, v.getName(), keyvalue);
            }
            ps.setObject(i, converterFactory.convertObject(
              keyvalue, v != null ? v.getJdbc_type() : null
            ));
            i = i + 1;
          }
        }

        if (tdf != null && tdf.hasOwner_id_column()) {
          TableColumn v = updateRequest.findColumn(tdf.getOwner_id_column().getName());
          if (log.isDebugEnabled()) {
            log.debug("设置上下文条件参数: PI={}, COLUMN={}, VALUE={}", i, v.getName(), input_data.get(v.getName()));
          }
          ps.setObject(i, converterFactory.convertObject(input_data.get(v.getName()), v.getJdbc_type()));
          i = i + 1;
        }

        if (tdf != null && tdf.hasDefault_where()) {
          List<Object> where_conditions = new LinkedList<>();
          List<WhereCloumn> copied_where = tdf.copyDefault_where();
          for (WhereCloumn w : copied_where) {
            w.buildParameters(where_conditions, converterFactory);
          }
          for (Object where_object : where_conditions) {
            ps.setObject(i, where_object);
            if (log.isDebugEnabled()) {
              log.debug("设置缺省定义条件参数: PI={}, WHERE={}", i, where_object);
            }
            i = i + 1;
          }
        }

        ps.addBatch();
      }
      return ps.executeBatch();
    } finally {
      JdbcUtils.closeStatement(ps);
    }
  }


}
