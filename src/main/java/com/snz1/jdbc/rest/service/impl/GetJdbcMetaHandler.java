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
    JdbcMetaData jdbc_meta = JdbcUtils.getJdbcMetaData(conn);
    jdbc_meta.setJdbc_url(this.getAppInfoResolver().getJdbcURL());
    jdbc_meta.setJdbc_user(this.getAppInfoResolver().getJdbcUser());
    jdbc_meta.setSso_enabled(this.getAppInfoResolver().isSsoEnabled());
    jdbc_meta.setPredefined_enabled(this.getAppInfoResolver().isPredefinedEnabled());
    jdbc_meta.setDynamic_config_type(this.getAppInfoResolver().getDynamiConfigType());
    jdbc_meta.setCache_type(this.getAppInfoResolver().getCacheType());
    return jdbc_meta;
  }

}
