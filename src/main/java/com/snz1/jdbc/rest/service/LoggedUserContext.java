package com.snz1.jdbc.rest.service;

import com.snz1.web.security.User;

import gateway.sc.v2.User.IdType;

// 登录用户上下文
public interface LoggedUserContext {

  /**
   * 获得当前登录用户对象
   *
   * @return {@link User}
   */
  User getLoggedUser();

  // 用户是否登录
  boolean isUserLogged();

  // 获取登录用户ID
  String getLoggedUsername();

  // 获取登录用户标识
  String getLoggedIdByType(IdType idtype);

  // 获取登录用户姓名
  String getLoggedName();
  

}
