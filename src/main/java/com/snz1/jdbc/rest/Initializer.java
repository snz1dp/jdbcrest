package com.snz1.jdbc.rest;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.springframework.boot.ConfigurableBootstrapContext;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringApplicationRunListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;

import com.snz1.jdbc.rest.data.JdbcMetaData;
import com.snz1.jdbc.rest.service.CacheClear;
import com.snz1.jdbc.rest.utils.JdbcUtils;
import com.snz1.scheme.FileConfigDataSchemaManager;
import com.snz1.utils.Configurer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Initializer implements SpringApplicationRunListener {

  private boolean cacheAble;
  
  private boolean dataSchemeEnabled;

  public Initializer(SpringApplication app, String ...args) {
  }

  @Override
  public void starting(ConfigurableBootstrapContext ctx) {
  }

  private void createDatabase(ConfigurableEnvironment environment) {
    String databaseDriver = environment.getProperty("spring.datasource.driver-class-name", "");
    Validate.notBlank(databaseDriver, "未设置数据库驱动");
    Driver jdbc_driver = null;
    try {
      @SuppressWarnings("unchecked")
      Class<Driver> driver_clazz = (Class<Driver>) Class.forName(databaseDriver);
      jdbc_driver = driver_clazz.newInstance();
    } catch(InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      throw new IllegalStateException("找不到数据库驱动类: " + databaseDriver);
    }

    String databaseUsername = environment.getProperty("spring.datasource.admin.username");
    String databasePassword = environment.getProperty("spring.datasource.admin.password");
    if (StringUtils.isBlank(databaseUsername)) {
      databaseUsername = environment.getProperty("spring.datasource.username");
      databasePassword = environment.getProperty("spring.datasource.password");
    }
    
    String databaseJdbcURL = environment.getProperty("spring.datasource.url", "");
    String databaseName = environment.getProperty("spring.datasource.create.database", "");

    Connection conn;
    try {
      Properties user_and_password = new Properties();
      if (databaseUsername != null) {
         user_and_password.put("user", databaseUsername);
      }

      if (databasePassword != null) {
        user_and_password.put("password", databasePassword);
      }
      conn = jdbc_driver.connect(databaseJdbcURL, user_and_password);
    } catch (SQLException e) {
      throw new IllegalStateException("创建数据库连接失败: " + e.getMessage(), e);
    }

    try {
      JdbcMetaData jdbc_meta;
      try {
        jdbc_meta = JdbcUtils.getJdbcMetaData(conn);
      } catch (SQLException e) {
        throw new IllegalStateException("获取数据库信息失败: " + e.getMessage(), e);
      }

      String sql_dialect_clazz_name = jdbc_meta.getProvider_class();
      Class<?> sql_dialect_clazz;
      try {
        sql_dialect_clazz = ClassUtils.getClass(sql_dialect_clazz_name);
      } catch(ClassNotFoundException e) {
        throw new IllegalStateException(String.format("暂时不支持%s数据库: %s", jdbc_meta.getProduct_name(), e.getMessage()), e);
      }
      
      // 设置与数据库相关的环境变量
      Method database_env_setup = MethodUtils.getAccessibleMethod(
        sql_dialect_clazz,
        "setupDatabaseEnvironment",
        ConfigurableEnvironment.class
      );
      try {
        database_env_setup.invoke(
          null, environment
        );
      } catch (IllegalAccessException | InvocationTargetException e) {
        log.warn(e.getMessage(), e);
        throw new IllegalStateException(e.getMessage(), e);
      }

      if (StringUtils.isNotBlank(databaseName)) {
        // 创建数据库
        Method create_database_if_not_existed = MethodUtils.getAccessibleMethod(
          sql_dialect_clazz,
          "createDatabaseIfNotExisted",
          Connection.class,
          String.class, String.class, String.class
        );
        try {
          create_database_if_not_existed.invoke(
            null, conn, databaseName, databaseUsername, databasePassword
          );
        } catch (IllegalAccessException | InvocationTargetException e) {
          log.warn(e.getMessage(), e);
          throw new IllegalStateException(e.getMessage(), e);
        }
      }
    } finally {
      JdbcUtils.closeConnection(conn);
    }

  }

  @Override
  public void environmentPrepared(ConfigurableBootstrapContext bootstrapContext, ConfigurableEnvironment environment) {
    createDatabase(environment);
    String cache_type = environment.getProperty("spring.cache.type", "none");
    this.cacheAble = !StringUtils.equals(cache_type, "none");
    String datascheme_enabled = environment.getProperty("spring.datascheme.enabled", "false");
    this.dataSchemeEnabled = StringUtils.equalsIgnoreCase(datascheme_enabled, "true");
  }

  @Override
  public void contextPrepared(ConfigurableApplicationContext context) {
  }

  @Override
  public void contextLoaded(ConfigurableApplicationContext context) {
  }

  @Override
  public void started(ConfigurableApplicationContext context) {
    if (!this.cacheAble) {
      log.warn("未启用高速缓存功能，忽略缓存清理!!");
      return;
    }

    if (!this.dataSchemeEnabled) {
      log.warn("未启用数据结构自动构建，忽略缓存清理!!");
    }

    FileConfigDataSchemaManager schema_mgr = context.getBean(FileConfigDataSchemaManager.class);
    String configed_version = Configurer.getAppProperty("data.scheme.version", "0");
    Integer inconfig_version = 0;

    try {
      inconfig_version = Integer.parseInt(configed_version);
    } catch(Throwable e) {}
    if (inconfig_version == schema_mgr.getVersion()) {
      log.warn("配置中的数据结构版本等于当前数据结构版本V{}，忽略缓存清理!", inconfig_version);
      return;
    }

    long start_time = System.currentTimeMillis();
    if (log.isInfoEnabled()) {
      log.info("开始清理高速缓存(O=V{}, N=V{})...", inconfig_version, schema_mgr.getVersion());
    }

    context.getBeansOfType(CacheClear.class).forEach((k, v) -> {
      v.clearCaches();
    });

    Configurer.setAppProperty("data.scheme.version", schema_mgr.getVersion() + "");

    if (log.isInfoEnabled()) {
      log.info(
        "清理高速缓存(O=V{}, N=V{})耗时{}毫秒",
        inconfig_version,
        schema_mgr.getVersion(),
        (System.currentTimeMillis() - start_time)
      );
    }
  }

  @Override
  public void running(ConfigurableApplicationContext context) {
  }

  @Override
  public void failed(ConfigurableApplicationContext context, Throwable exception) {
  }

}
