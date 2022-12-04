package com.snz1.jdbc.rest;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.springframework.boot.ConfigurableBootstrapContext;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringApplicationRunListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;

import com.snz1.jdbc.rest.data.JdbcMetaData;
import com.snz1.jdbc.rest.utils.JdbcUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Initializer implements SpringApplicationRunListener {

  public Initializer(SpringApplication app, String ...args) {
  }

  @Override
  public void starting(ConfigurableBootstrapContext ctx) {
  }

  private void createDatabase(ConfigurableEnvironment environment) {
    String databaseUsername = environment.getProperty("spring.datasource.admin.username", "postgres");
    String databasePassword = environment.getProperty("spring.datasource.admin.password", "123456");
    String databaseJdbcURL = environment.getProperty("spring.datasource.url", "");

    String databaseName = environment.getProperty("spring.datasource.create.database", "");
    if (StringUtils.isBlank(databaseName)) return;

    Connection conn;
    try {
      conn = DriverManager.getConnection(databaseJdbcURL, databaseUsername, databasePassword);
    } catch (SQLException e) {
      throw new IllegalStateException("创建数据库连接失败: " + e.getMessage(), e);
    }

    JdbcMetaData jdbc_meta;
    try {
      jdbc_meta = JdbcUtils.getJdbcMetaData(conn);
    } catch (SQLException e) {
      throw new IllegalStateException("获取数据库信息失败: " + e.getMessage(), e);
    }

    String sql_dialect_clazz_name = String.format(
      "%s.service.%s.SQLDialectProvider",
      getClass().getPackage().getName(), 
      jdbc_meta.getProduct_name().toLowerCase()
    );

    Class<?> sql_dialect_clazz;
    try {
      sql_dialect_clazz = ClassUtils.getClass(sql_dialect_clazz_name);
    } catch(ClassNotFoundException e) {
      throw new IllegalStateException(String.format("暂不支持%s:%s", jdbc_meta.getProduct_name(), e.getMessage()), e);
    }

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

  @Override
  public void environmentPrepared(ConfigurableBootstrapContext bootstrapContext, ConfigurableEnvironment environment) {
    createDatabase(environment);
  }

  @Override
  public void contextPrepared(ConfigurableApplicationContext context) {
  }

  @Override
  public void contextLoaded(ConfigurableApplicationContext context) {
  }

  @Override
  public void started(ConfigurableApplicationContext context) {
  }

  @Override
  public void running(ConfigurableApplicationContext context) {
  }

  @Override
  public void failed(ConfigurableApplicationContext context, Throwable exception) {
  }

}
