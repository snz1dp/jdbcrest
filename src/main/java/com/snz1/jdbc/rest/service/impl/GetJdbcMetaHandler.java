package com.snz1.jdbc.rest.service.impl;

import java.sql.Connection;
import java.sql.SQLException;

import org.springframework.dao.DataAccessException;
import org.springframework.lang.Nullable;

import com.snz1.jdbc.rest.data.JdbcMetaData;
import com.snz1.jdbc.rest.service.AppInfoResolver;
import com.snz1.jdbc.rest.utils.JdbcUtils;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class GetJdbcMetaHandler extends AbstractJdbcQueryRequestHandler<JdbcMetaData> {

  public GetJdbcMetaHandler(AppInfoResolver appInfoResolver) {
    super(null, null, null, null, appInfoResolver);
  }

  @Override
  @Nullable
  public JdbcMetaData doInConnection(Connection conn) throws SQLException, DataAccessException {
    return JdbcUtils.getJdbcMetaData(conn);
  }

}
