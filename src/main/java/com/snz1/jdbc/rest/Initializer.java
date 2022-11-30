package com.snz1.jdbc.rest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.ConfigurableBootstrapContext;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringApplicationRunListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Initializer implements SpringApplicationRunListener {

  public Initializer(SpringApplication app, String ...args) {
  }

  @Override
  public void starting(ConfigurableBootstrapContext ctx) {
  }

  private void createDatabase(ConfigurableEnvironment environment) {
    String databaseUsername = environment.getProperty("spring.datasource.username", "postgres");
    String databasePassword = environment.getProperty("spring.datasource.password", "123456");
    String databaseJdbcURL = environment.getProperty("JDBC_URL", "");
    
    // String databaseName = environment.getProperty("PG_DATABASE", "ingress");

    // Connection conn;
    // try {
    //   conn = DriverManager.getConnection(databaseJdbcURL, databaseUsername, databasePassword);
    // } catch (SQLException e) {
    //   throw new IllegalStateException("创建数据库连接失败: " + e.getMessage(), e);
    // }
    // ResultSet result = null;
    // try {
    //   Statement stmt = conn.createStatement();
    //   result = stmt.executeQuery(String.format("SELECT u.datname FROM pg_catalog.pg_database u where u.datname='%s';", databaseName));
    //   if (!result.next()) {
    //     String createSQL = String.format("CREATE DATABASE %s WITH OWNER %s;", databaseName, databaseUsername);
    //     log.info("执行SQL语句: " + createSQL);
    //     stmt.execute(createSQL);
    //   }
    // } catch(SQLException e) {
    //   if (!StringUtils.contains(e.getMessage(), "already exists")) {
    //     throw new IllegalStateException("创建数据库失败: " + e.getMessage(), e);
    //   }
    // } finally {
    //   try {
    //     if (result != null) {
    //       result.close();
    //     }
    //     conn.close();
    //   } catch(Throwable e) {
    //   }
    // }

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
