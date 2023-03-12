package com.snz1.jdbc.rest.service.impl;

import javax.annotation.Resource;
import java.util.Set;
import java.util.List;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.apache.commons.lang3.StringUtils;

import com.snz1.jdbc.rest.service.UserRoleVerifier;
import com.snz1.jdbc.rest.Constants;
import com.snz1.jdbc.rest.service.AppInfoResolver;
import com.snz1.web.security.User;

import gateway.sc.v2.UserManager;
import gateway.sc.v2.User.IdType;
import gateway.sc.v2.FunctionManager;
import gateway.sc.v2.FunctionNode;
import gateway.api.Page;

@Component
public class UserRoleVerifierImpl implements UserRoleVerifier {

  @Resource
  private AppInfoResolver appInfoResolver;

  @Resource
  private UserManager userManager;

  @Resource
  private FunctionManager functionManager;

  @Value("${app.allapp.role:}")
  private String allAppRoleCode;

  protected boolean hasAllAppRoleCode() {
    return StringUtils.isNotBlank(this.allAppRoleCode);
  }

  private List<String> doLoadUserRoleList(String userid) {
    List<String> role_list = userManager.getUserRoles(
      userid, IdType.id, null,
      true,
      appInfoResolver.getAppId()
    );
    if (role_list == null) return Collections.emptyList();
    return role_list;
  }

  @Override
  public boolean isUserInAnyRole(User logged_user, String... roles) {
    List<String> role_list = null;
    if (logged_user instanceof gateway.sc.v2.User) {
      role_list = ((gateway.sc.v2.User)logged_user).getRoles();
      if (role_list == null) {
        ((gateway.sc.v2.User)logged_user).setRoles(
          role_list = doLoadUserRoleList(logged_user.getUserid())
        );
      }
    } else {
      role_list = logged_user.getRoles();
    }
    if (role_list == null || role_list.size() == 0) {
      return false;
    }
    Set<String> user_roles = new HashSet<>(role_list);
    for (String role : roles) {
      if (role == null || StringUtils.isBlank(role)) continue;
      if (user_roles.contains(role)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String[] getUserOwnerAppcodes(User logged_user, String... appcodes) {
    if (hasAllAppRoleCode() && isUserInAnyRole(logged_user, this.allAppRoleCode)) {
      if (appcodes == null || appcodes.length == 0) return null;
      return appcodes;
    };

    if (appcodes != null && appcodes.length > 0) {
      boolean first_empty = true;
      List<String> appcode_list = new LinkedList<>();
      for (String appcode : appcodes) {
        if (StringUtils.isBlank(appcode)) continue;
        first_empty = false;
        FunctionNode fnode = functionManager.getFunctionNode(appcode);
        if (fnode == null) continue;
        if (StringUtils.equals(logged_user.getUserid(), fnode.getManager()) ||
          StringUtils.equals(logged_user.getUserid(), fnode.getLeader()) ||
          StringUtils.equals(logged_user.getUserid(), fnode.getMaintenancer()) ||
          (
            fnode.getMaintenancers() != null &&
            new HashSet<String>(fnode.getMaintenancers()).contains(logged_user.getUserid())
          )
        ) {
          appcode_list.add(appcode);
        }
      }
      if (!first_empty)
        return appcode_list.size() > 0 ? appcode_list.toArray(new String[0]) : Constants.NO_APP_CODES;
    }

    int start = 0;
    Page<FunctionNode> app_page = null;
    Set<String> app_codes = new HashSet<>();
    do {
      app_page = functionManager.getFunctionNodes(
        null,
        null,
        FunctionNode.Type.app,
        false,
        false,
        logged_user.getUserid(),
        IdType.id,
        false,
        start,
        100
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
  public String[] getUserApprovalAppcodes(User logged_user, String... appcodes) {
    if (hasAllAppRoleCode() && isUserInAnyRole(logged_user, this.allAppRoleCode)) {
      if (appcodes == null || appcodes.length == 0) return null;
      return appcodes;
    }

    if (appcodes != null && appcodes.length > 0) {
      boolean first_empty = true;
      List<String> appcode_list = new LinkedList<>();
      for (String appcode : appcodes) {
        if (StringUtils.isBlank(appcode)) continue;
        first_empty = false;
        FunctionNode fnode = functionManager.getFunctionNode(appcode);
        if (fnode == null) continue;
        if (StringUtils.equals(logged_user.getUserid(), fnode.getManager())) {
          appcode_list.add(appcode);
        }
      }
      if (!first_empty)
        return appcode_list.size() > 0 ? appcode_list.toArray(new String[0]) : Constants.NO_APP_CODES;
    }

    int start = 0;
    Page<FunctionNode> app_page = null;
    Set<String> app_codes = new HashSet<>();
    do {
      app_page = functionManager.getFunctionNodes(
        null,
        null,
        FunctionNode.Type.app,
        false,
        false,
        logged_user.getUserid(),
        IdType.id,
        false,
        start, 100
      );
      if (app_page.data == null || app_page.data.size() == 0) {
        break;
      }

      app_page.data.forEach(app -> {
        if (StringUtils.equals(logged_user.getUserid(), app.getManager())) {
          app_codes.add(app.getCode());
        }
      });

      start += 100;
    } while(start >= (int)app_page.total);

    if (app_codes.size() == 0) return Constants.NO_APP_CODES;

    return app_codes.toArray(new String[0]);
  }

  @Override
  public boolean isAppOwnerUser(String appcode, User logged_user) {
    if (StringUtils.isBlank(appcode)) return false;
    FunctionNode funcnode = functionManager.getFunctionNode(appcode);
    if (funcnode == null) return false;
    return StringUtils.equals(funcnode.getManager(), logged_user.getUserid()) ||
      StringUtils.equals(funcnode.getLeader(), logged_user.getUserid()) ||
      StringUtils.equals(funcnode.getMaintenancer(), logged_user.getUserid()) ||
      (
        funcnode.getMaintenancers() != null &&
        new HashSet<String>(funcnode.getMaintenancers()).contains(logged_user.getUserid())
      );
  }

  @Override
  public boolean isAppApprovalUser(String appcode, User logged_user) {
    if (StringUtils.isBlank(appcode)) return false;
    FunctionNode funcnode = functionManager.getFunctionNode(appcode);
    if (funcnode == null) return false;
    return StringUtils.equals(funcnode.getManager(), logged_user.getUserid());
  }

}
