package com.snz1.jdbc.rest.service.impl;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import org.springframework.dao.DataAccessException;
import org.springframework.lang.Nullable;

import com.snz1.jdbc.rest.data.JdbcMetaData;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class GetJdbcMetaHandler extends AbstractJdbcQueryRequestHandler<JdbcMetaData> {

  public GetJdbcMetaHandler() {
    super(null, null, null, null);
  }

  @Override
  @Nullable
  public JdbcMetaData doInConnection(Connection conn) throws SQLException, DataAccessException {
    JdbcMetaData temp_meta = new JdbcMetaData();
    DatabaseMetaData table_meta =  conn.getMetaData();
    temp_meta.setProduct_name(table_meta.getDatabaseProductName());
    temp_meta.setProduct_version(table_meta.getDatabaseProductVersion());
    temp_meta.setDriver_name(table_meta.getDriverName());
    temp_meta.setDriver_version(table_meta.getDriverVersion());
    return temp_meta;
  }

}
