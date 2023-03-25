package com.snz1.jdbc.rest;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.Objects;

import javax.annotation.Resource;

import gateway.sc.v2.PermissionDefinition;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.UrlResource;

import com.snz1.jdbc.rest.utils.FileUtils;
import com.snz1.utils.Configurer;
import com.snz1.utils.JsonUtils;

@Slf4j
public class RunConfig {

  @Autowired(required = false)
  private PermissionDefinition permissionDefinition;

  @Resource
  private Version appVerison;

  @Value("${server.context-path:/jdbc/rest/api}")
  private String webroot;

  @Value("${app.default_url:/swagger-ui/index.html}")
	private String defaultTargetUrl;

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

  private Date firstRunTime = new Date();

  public String getWebroot() {
    return webroot;
  }

  public boolean isGlobalReadonly() {
    return global_readonly;
  }

  public String getDefaultTargetUrl() {
    return defaultTargetUrl;
  }

  public String getApplicationCode() {
    return applicationCode;
  }

  public Version getAppVerison() {
    return appVerison;
  }

  public boolean isStrictMode() {
    return this.strict_mode;
  }

  public PermissionDefinition getPermissionDefinition() {
    return permissionDefinition;
  }

  public boolean hasPermissionDefinition() {
    return this.getPermissionDefinition() != null;
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

  public boolean isPersistenceConfig() {
    return !StringUtils.equals(this.config_type, "none");
  }

  public File getSql_location_dir() {
    String sql_location = this.getSql_location();
    if (StringUtils.isBlank(sql_location)) return null;
    File sql_directory;
    try {
      sql_directory = new UrlResource(sql_location).getFile();
    } catch (IOException e) {
      if (StringUtils.startsWith(sql_location, "classpath:")) {
        sql_location = StringUtils.substring(sql_location, 10);
      }
      try {
        sql_directory = new ClassPathResource(sql_location).getFile();
      } catch (IOException ex) {
        log.warn("无法获取SQL服务目录信息, Path={}, 错误信息: {}", sql_location, e.getMessage(), e);
        return null;
      }
    }
    return sql_directory;
  }

  public File getTable_definition_file() {
    String file_location = this.getTable_definition();
    if (StringUtils.isBlank(file_location)) {
      return null;
    }
    File tdf_resource;
    try {
      tdf_resource = new UrlResource(file_location).getFile();
    } catch (IOException e) {
      if (StringUtils.startsWith(file_location, "classpath:")) {
        file_location = StringUtils.substring(file_location, 10);
      }
      try {
        tdf_resource = new ClassPathResource(file_location).getFile();
      } catch (IOException ex) {
        log.warn("获取数据表配置文件信息失败, Path={}, 错误信息: {}", file_location, e.getMessage(), e);
        return null;
      }
    }
    return tdf_resource;
  }

  public Date getFirstRunTime() {
    if (this.isPersistenceConfig()) {
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
    } else {
      File temp_file = this.getTable_definition_file();
      if (temp_file != null) {
        try {
          return this.firstRunTime = FileUtils.getCreateTime(temp_file);
        } catch (IOException e) {}
      }
      temp_file = this.getSql_location_dir();
      if (temp_file != null) {
        try {
          return this.firstRunTime = FileUtils.getCreateTime(temp_file);
        } catch (IOException e) {}
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

}
