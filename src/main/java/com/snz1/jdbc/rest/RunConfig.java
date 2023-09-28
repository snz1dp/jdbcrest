package com.snz1.jdbc.rest;

import java.util.Date;
import java.util.Objects;

import javax.annotation.Resource;

import gateway.sc.v2.PermissionDefinition;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.snz1.utils.Configurer;
import com.snz1.utils.JsonUtils;

public class RunConfig {

  @Autowired(required = false)
  private PermissionDefinition permissionDefinition;

  @Resource
  private Version appVerison;

  @Value("${server.context-path:/jdbc/rest/api}")
  private String webroot;

  @Value("${app.default_url:/swagger-ui/index.html}")
	private String default_target_url;

  @Value("${app.code}")
  private String applicationCode;

  @Value("${app.license.code:${LICENSE_CODE:<NOT SUPPORT>}}")
  private String license_code;

  @Value("${app.deployment.id:${DEPLOYMENT_ID:}}")
  private String deployment_id;

  @Value("${app.sql.location:${SQL_LOCATION:}}")
  private String sql_location;

  @Value("${app.config.type:${CONFIG_TYPE:none}}")
  private String config_type;

  @Value("${spring.security.authorize:${SERVICE_AUTHORIZE:}}")
  private String service_authorize;

  @Value("${app.table.definition:${TABLE_DEFINITION:}}")
  private String table_definition;

  @Value("${app.name:${SERVICE_NAME:}}")
  private String service_name;

  @Value("${app.version:${SERVICE_VERSION:1.0.0}}")
  private String service_version;

  @Value("${app.support.group:${SUPPORT_GROUP:}}")
  private String support_group;

  @Value("${app.support.username:${SUPPORT_USERNAME:}}")
  private String support_username;

  @Value("${app.support.email:${SUPPORT_EMAIL:}}")
  private String support_email;

  @Value("${app.table.readonly:${READONLY_SERVICE:false}}")
  private boolean global_readonly;

  @Value("${app.strict.mode:${STRICT_MODE:false}}")
  private boolean strict_mode;

  @Value("${spring.datasource.url:${JDBC_URL:}}")
  private String jdbc_url;

  @Value("${spring.datasource.username:${JDBC_USER:}}")
  private String jdbc_user;

  @Value("${spring.security.ssoheader:${SSO_ENABLED:false}}")
  private Boolean sso_enabled;

  @Value("${app.predefined.enabled:${PERMISSION_ENABLED:false}}")
  private Boolean predefined_enabled;

  @Value("${app.config.type:${CONFIG_TYPE:none}}")
  private String dynamic_config_type;

  @Value("${spring.cache.type:${CACHE_TYPE:none}}")
  private String cache_type;

  @Value("${app.license.strict:false}")
  private Boolean strict_license;

  @Value("${app.user.scope:employee}")
  private String default_user_scope;

  @Value("${spring.database.schema:}")
  private String default_database_schema;

  private Date firstRunTime = new Date();

  public String getWebroot() {
    return webroot;
  }

  public String getDefault_database_schema() {
    return this.default_database_schema;
  }

  public boolean isGlobal_readonly() {
    return global_readonly;
  }

  public String getDefault_target_url() {
    return default_target_url;
  }

  public String getApplication_code() {
    return applicationCode;
  }

  public Version getApp_verison() {
    return appVerison;
  }

  public boolean isStrict_mode() {
    return this.strict_mode;
  }

  public PermissionDefinition getPermission_definition() {
    return permissionDefinition;
  }

  public boolean hasPermission_definition() {
    return this.getPermission_definition() != null;
  }

  public String getSql_location() {
    return sql_location;
  }

  public String getTable_definition() {
    return table_definition;
  }

  public String getLicense_code() {
    return license_code;
  }

  public String getDeployment_id() {
    return deployment_id;
  }

  public boolean isPersistence_config() {
    return !StringUtils.equals(this.config_type, "none");
  }

  public Date getFirst_run_time() {
    if (this.isPersistence_config()) {
      String installed_time = Configurer.getAppProperty("first_run_time", null);
      if (installed_time == null) {
        Configurer.setAppProperty("first_run_time", JsonUtils.toJson(this.firstRunTime));
      } else {
        try {
          Date first_run_time = JsonUtils.fromJson(installed_time, Date.class);
          if (!Objects.equals(this.firstRunTime, first_run_time)) {
            this.firstRunTime = first_run_time;
          }
        } catch(Throwable e) {
          Configurer.setAppProperty("first_run_time", JsonUtils.toJson(this.firstRunTime));
        }
      }
    }
    return this.firstRunTime;
  }

  public String getService_version() {
    return service_version;
  }

  public String getService_name() {
    return service_name;
  }

  public String getJdbc_url() {
    return jdbc_url;
  }

  public String getJdbc_user() {
    return jdbc_user;
  }

  public Boolean getSso_enabled() {
    return sso_enabled;
  }

  public Boolean getPredefined_enabled() {
    return predefined_enabled;
  }

  public String getDynamic_config_type() {
    return dynamic_config_type;
  }

  public String getCache_type() {
    return cache_type;
  }

  public String getSupport_group() {
    return support_group;
  }

  public String getSupport_email() {
    return support_email;
  }

  public String getSupport_username() {
    return support_username;
  }

  public Boolean getStrict_license() {
    return strict_license;
  }

  public String getDefault_user_scope() {
    return default_user_scope;
  }

}
