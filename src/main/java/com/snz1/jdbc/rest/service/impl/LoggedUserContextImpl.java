package com.snz1.jdbc.rest.service.impl;

import com.snz1.jdbc.rest.service.LoggedUserContext;
import com.snz1.utils.ContextUtils;
import com.snz1.web.security.User;

import gateway.sc.v2.User.IdType;


public class LoggedUserContextImpl implements LoggedUserContext {

  @Override
  public User getLoggedUser() {
    return ContextUtils.getRequestLoginedUser();
  }

  @Override
  public boolean isUserLogged() {
    return ContextUtils.getRequestLoginedUser() != null;
  }

  @Override
  public String getLoggedIdByType(IdType idtype) {
    User logged_user = getLoggedUser();
    if (logged_user == null) return null;
    String userid = null;
    switch(idtype) {
      case uname:
        userid = logged_user.getAccount_name();
        break;
      case mobi:
        userid = logged_user.getRegist_mobile();
        break;
      case email:
        userid = logged_user.getRegist_email();
        break;
      default:
        userid = logged_user.getUserid();
        break;
    }
    return userid;
  }

  @Override
  public String getLoggedUsername() {
    return this.getLoggedIdByType(IdType.uname);
  }

  @Override
  public String getLoggedName() {
    User logged_user = getLoggedUser();
    if (logged_user == null) return null;
    return logged_user.getDisplay_name();
  }

}
