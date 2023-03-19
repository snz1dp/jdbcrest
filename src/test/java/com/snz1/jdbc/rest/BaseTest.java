package com.snz1.jdbc.rest;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;


// 使用基类定义测试配置文件
@SpringBootTest(classes = BaseTest.Application.class, properties = { //
  "spring.profiles.active=test"
})
public class BaseTest {

  @SpringBootApplication
  @Configuration
  public static class Application {
  }

}
