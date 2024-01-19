package com.snz1.jdbc.rest.service.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

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

@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class DeleteRequestHandler extends AbstractManipulationRequestHandler<Integer> {

  public DeleteRequestHandler(
    ManipulationRequest update_request,
    SQLDialectProvider sql_dialect_provider,
    JdbcTypeConverterFactory type_converter_factory,
    LoggedUserContext loggedUserContext,
    AppInfoResolver appInfoResolver,
    BeanUtilsBean bean_utils
  ) {
    super(update_request, sql_dialect_provider, type_converter_factory, loggedUserContext, appInfoResolver, bean_utils);
  }

  @Override
  @Nullable
  public Integer doInConnection(Connection conn) throws SQLException, DataAccessException {
    ManipulationRequest deleteRequest = this.getRequest();
    TableDefinition tdf = deleteRequest.getDefinition();

    SQLDialectProvider sqlDialectProvider = this.getSqlDialectProvider();
    JdbcTypeConverterFactory converterFactory = this.getConverterFactory();
    LoggedUserContext loggedUserContext = this.getLoggedUserContext();
    JdbcQueryStatement sts = sqlDialectProvider.prepareDataDelete(deleteRequest);
    PreparedStatement ps = null;
    try {
      ps = conn.prepareStatement(sts.getSql());
      int i = 1;

      for (Object where_object : sts.getParameters()) {
        ps.setObject(i, where_object);
        i = i + 1;
      }

      if (!deleteRequest.hasWhere()) {
        Object keyvalue = deleteRequest.getInput_key();
        Object rowkey = deleteRequest.getRow_key();
        if (deleteRequest.testComposite_key()) {
          List<?> row_keys = (List<?>)rowkey;
          List<?> key_values = (List<?>)keyvalue;
          for (int j = 0; j < row_keys.size(); j++) {
            String keyname = (String)row_keys.get(j);
            Object keyval = key_values.get(j);
            TableColumn keycol = deleteRequest.findColumn(keyname);
            ps.setObject(i, converterFactory.convertObject(
              keyval, keycol != null ? keycol.getJdbc_type() : null
            ));
            i = i + 1;
          }
        } else {
          TableColumn keycol = deleteRequest.findColumn((String)rowkey);
          ps.setObject(i, converterFactory.convertObject(
            keyvalue, keycol != null ? keycol.getJdbc_type() : null
          ));
          i = i + 1;
        }
      }

      if (tdf != null && tdf.hasOwner_id_column()) {
        if (loggedUserContext.isUserLogged()) {
          ps.setObject(i, loggedUserContext.getLoggedIdByType(tdf.getOwner_id_column().getIdtype()));
        } else {
          ps.setObject(i, null);
        }
        i = i + 1;
      }

      return ps.executeUpdate();
    } finally {
      JdbcUtils.closeStatement(ps);
    }
  }
    
}
