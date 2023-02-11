package com.snz1.jdbc.rest.service.impl;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Resource;

import com.snz1.jdbc.rest.Constants;
import com.snz1.jdbc.rest.RunConfig;
import com.snz1.jdbc.rest.service.LoggedUserContext;
import com.snz1.utils.ContextUtils;
import com.snz1.web.security.User;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContext;

import gateway.api.Page;
import gateway.sc.v2.FunctionManager;
import gateway.sc.v2.FunctionNode;
import gateway.sc.v2.UserManager;
import gateway.sc.v2.User.IdType;

public class LoggedUserContextImpl implements LoggedUserContext {

  @Resource
  private UserManager userManager;

  @Resource
  private FunctionManager functionManager;

  @Resource
  private RunConfig runConfig;

  @Value("${app.allapp.role:}")
  private String allAppRoleCode;

  protected boolean hasAllAppRoleCode() {
    return StringUtils.isNotBlank(this.allAppRoleCode);
  }

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

  @Override
  public String[] getUserOwnerAppcodes(String appcode) {
    User logged_user = getLoggedUser();
    if (logged_user == null) return Constants.NO_APP_CODES;
    if (hasAllAppRoleCode() && hasRole(this.allAppRoleCode)) {
      if (StringUtils.isBlank(appcode)) return null;
      return new String[] {appcode};
    };

    if (StringUtils.isNotBlank(appcode)) {
      FunctionNode fnode = functionManager.getFunctionNode(appcode);
      if (fnode == null) return Constants.NO_APP_CODES;
      return (StringUtils.equals(logged_user.getUserid(), fnode.getManager()) ||
        StringUtils.equals(logged_user.getUserid(), fnode.getLeader()) ||
        StringUtils.equals(logged_user.getUserid(), fnode.getMaintenancer()) ||
        (
          fnode.getMaintenancers() != null &&
          new LinkedHashSet<String>(fnode.getMaintenancers()).contains(logged_user.getUserid())
        )
      ) ? new String[]{ appcode } : Constants.NO_APP_CODES;
    }

    int start = 0;
    Page<FunctionNode> app_page = null;
    Set<String> app_codes = new LinkedHashSet<>();
    do {
      app_page = functionManager.getFunctionNodes(
        null, null,
        FunctionNode.Type.app,
        false, false,
        logged_user.getUserid(),
        IdType.id, false,
        start, 100
      );
      if (app_page.data == null || app_page.data.size() == 0) {
        break;
      }

      app_page.data.forEach(app -> {
        app_codes.add(app.getCode());
      });

      start += 100;
    } while(start >= (int)app_page.total);

    if (app_codes.size() == 0) return Constants.NO_APP_CODES;

    return app_codes.toArray(new String[0]);
  }

  @Override
  public boolean testAppOwnerUser(String appcode) {
    User logged_user = getLoggedUser();
    if (logged_user == null || StringUtils.isBlank(appcode)) return false;

    FunctionNode funcnode = functionManager.getFunctionNode(appcode);
    if (funcnode == null) return false;
    return StringUtils.equals(funcnode.getManager(), logged_user.getUserid()) ||
      StringUtils.equals(funcnode.getLeader(), logged_user.getUserid()) ||
      StringUtils.equals(funcnode.getMaintenancer(), logged_user.getUserid()) ||
      (
        funcnode.getMaintenancers() != null &&
        new LinkedHashSet<String>(funcnode.getMaintenancers()).contains(logged_user.getUserid())
      );
  }

}
