package com.snz1.jdbc.rest.service.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.beanutils.BeanUtilsBean;
import org.springframework.dao.DataAccessException;
import org.springframework.lang.Nullable;

import com.snz1.jdbc.rest.data.JdbcQueryStatement;
import com.snz1.jdbc.rest.data.ManipulationRequest;
import com.snz1.jdbc.rest.data.TableColumn;
import com.snz1.jdbc.rest.data.TableDefinition;
import com.snz1.jdbc.rest.provider.SQLDialectProvider;
import com.snz1.jdbc.rest.service.AppInfoResolver;
import com.snz1.jdbc.rest.service.JdbcTypeConverterFactory;
import com.snz1.jdbc.rest.service.LoggedUserContext;
import com.snz1.jdbc.rest.utils.JdbcUtils;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class InsertRequestHandler extends AbstractManipulationRequestHandler<Object> {

  public InsertRequestHandler(
    ManipulationRequest update_request,
    SQLDialectProvider sql_dialect_provider,
    JdbcTypeConverterFactory type_converter_factory,
    LoggedUserContext loggedUserContext,
    AppInfoResolver appInfoResolver,
    BeanUtilsBean bean_utils
  ) {
    super(update_request, sql_dialect_provider, type_converter_factory, loggedUserContext, appInfoResolver, bean_utils);
  }

  // 构建
  private Map<String, Object> doBuildInsertRequestInputData(Map<String, Object> input_data) {
    ManipulationRequest insertRequest = this.getRequest();
    TableDefinition definition = insertRequest.getDefinition();

    if (definition == null) {
      return input_data;
    }

    LoggedUserContext loggedUserContext = this.getLoggedUserContext();

    input_data = new HashMap<>(input_data);
    if (definition.hasCreated_time_column()) {
      input_data.put(definition.getCreated_time_column(), insertRequest.getRequest_time());
    }
    if (definition.hasUpdated_time_column()) {
      input_data.put(definition.getUpdated_time_column(), insertRequest.getRequest_time());
    }

    if (definition.hasCreator_id_column()) {
      TableDefinition.UserIdColumn userid = definition.getCreator_id_column();
      input_data.put(userid.getName(), loggedUserContext.getLoggedIdByType(userid.getIdtype()));
    }

    if (definition.hasCreator_name_column()) {
      input_data.put(definition.getCreator_name_column(), loggedUserContext.getLoggedName());
    }

    if (definition.hasMender_id_column()) {
      TableDefinition.UserIdColumn userid = definition.getMender_id_column();
      input_data.put(userid.getName(), loggedUserContext.getLoggedIdByType(userid.getIdtype()));
    }

    if (definition.hasMender_name_column()) {
      input_data.put(definition.getMender_name_column(), loggedUserContext.getLoggedName());
    }
    return input_data;
  }

  @Override
  @Nullable
  public Object doInConnection(Connection conn) throws SQLException, DataAccessException {
    ManipulationRequest insertRequest = this.getRequest();

    SQLDialectProvider sqlDialectProvider = this.getSqlDialectProvider();
    JdbcTypeConverterFactory converterFactory = this.getConverterFactory();
    JdbcQueryStatement base_query = sqlDialectProvider.prepareDataInsert(insertRequest);
    PreparedStatement ps = null;

    try {
      ps = conn.prepareStatement(base_query.getSql());
      List<Map<String, Object>> input_datas = insertRequest.getInput_list();
      for (Map<String, Object> input_data : input_datas) {
        input_data = doBuildInsertRequestInputData(input_data);
        int i = 1;
        if (insertRequest.hasColumns()) {
          for (TableColumn v : insertRequest.getColumns()) {
            if (v.getAuto_increment() != null && v.getAuto_increment()) continue;
            if (log.isDebugEnabled()) {
              log.debug("字段名 = {}", v.getName());
            }
            Object val = converterFactory.convertObject(input_data.get(v.getName()), v.getJdbc_type());
            try {
              ps.setObject(i, val);
            } catch(SQLException e) {
              String error_message = String.format(
                "参数%d, 字段名 = %s, 参数类 = %s",
                i, v.getName(), val == null ? "null" : val.getClass().getName()
              );
              throw new IllegalStateException(error_message, e);
            }
            i = i + 1;
          }
        } else {
          for (Map.Entry<String, Object> input_entry : input_data.entrySet()) {
            Object val = converterFactory.convertObject(input_entry.getValue(), null);
            ps.setObject(i, val);
            i = i + 1;
          }
        }
        ps.addBatch();
      }
      int inserted[] = ps.executeBatch();
      if (!insertRequest.hasAutoGenerated()) {
        return inserted;
      }
      ResultSet auto_keys = ps.getGeneratedKeys();
      List<Integer> key_list = new LinkedList<>();
      while(auto_keys.next()) {
        key_list.add(auto_keys.getInt(1));
      }
      return new Object[] {
        inserted, key_list.toArray(new Integer[0])
      };
    } finally {
      JdbcUtils.closeStatement(ps);
    }
  }

}
