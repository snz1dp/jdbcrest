package com.snz1.jdbc.rest.service.impl;

import java.util.Collection;
import java.util.List;

import javax.annotation.Resource;

import com.snz1.jdbc.rest.RunConfig;
import com.snz1.jdbc.rest.service.LoggedUserContext;
import com.snz1.utils.ContextUtils;
import com.snz1.web.security.User;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.stereotype.Component;

import gateway.sc.v2.FunctionManager;
import gateway.sc.v2.UserManager;

@Component
public class LoggedUserContextImpl implements LoggedUserContext {

  @Resource
  private UserManager userManager;

  @Resource
  private FunctionManager functionManager;

  @Resource
  private RunConfig runConfig;

  @Override
  public User getLoggedUser() {
    return ContextUtils.getRequestLoginedUser();
  }

  @Override
  public boolean isUserLogged() {
    return ContextUtils.getRequestLoginedUser() != null;
  }

  @Override
  public UserInfo getLoginUserInfo(boolean load_roles) {
    User logged_user = getLoggedUser();
    Validate.notNull(logged_user, "用户尚未登录");
    gateway.sc.v2.User user = userManager.getUser(
      logged_user.getUserid(), //
      gateway.sc.v2.User.IdType.id,
      false, false
    );
    List<String> roles = null;
    if (load_roles) {
      roles = userManager.getUserRoles(
        logged_user.getUserid(), //
        gateway.sc.v2.User.IdType.id,
        true, runConfig.getApplicationCode()
      );
    }
    return new UserInfo(user, logged_user.getAuth_mode(), logged_user.getScope(), roles);
  }

  @Override
  public UserInfo getLoginUserInfo() {
    User logged_user = getLoggedUser();
    Validate.notNull(logged_user, "用户尚未登录");
    gateway.sc.v2.User user = userManager.getUser(
      logged_user.getUserid(), //
      gateway.sc.v2.User.IdType.id,
      false, false
    );
    List<String> roles = userManager.getUserRoles(
      logged_user.getUserid(), //
      gateway.sc.v2.User.IdType.id,
      true, runConfig.getApplicationCode()
    );
    return new UserInfo(user, logged_user.getAuth_mode(), logged_user.getScope(), roles);
  }

  @Override
  public boolean hasRole(String role) {
    SecurityContext sc = ContextUtils.getSecurityContext();
    if (sc == null || sc.getAuthentication() == null || sc.getAuthentication().getAuthorities() == null) {
      return false;
    }
    Collection<? extends GrantedAuthority> authorities = ContextUtils.getSecurityContext().getAuthentication().getAuthorities();
    for (GrantedAuthority authority : authorities) {
      if (!StringUtils.startsWith(role, "ROLE_")) role = "ROLE_" + role;
      if (StringUtils.equals(authority.getAuthority(), role)) return true;
    }
    return false;
  }

  @Override
  public boolean hasAnyRole(String... roles) {
    SecurityContext sc = ContextUtils.getSecurityContext();
    if (sc == null || sc.getAuthentication() == null || sc.getAuthentication().getAuthorities() == null) {
      return false;
    }
    for (String role : roles) {
      if (hasRole(role)) return true;
    }
    return false;
  }

}
