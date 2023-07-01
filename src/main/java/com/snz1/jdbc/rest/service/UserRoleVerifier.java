package com.snz1.jdbc.rest.service;

import com.snz1.web.security.User;

// 用户角色验证器
public interface UserRoleVerifier {

  // 是否有任何权限
  boolean isUserInAnyRole(User logged_user, String ...roles);

  // 获取用户所属的应用代码
  String[] getUserOwnerAppcodes(User logged_user, String...appcode);

  // 获取用户审批的应用代码
  String[] getUserApprovalAppcodes(User logged_user, String...appcode);

  // 是否应用所属用户
  boolean isAppOwnerUser(String appcode, User logged_user);

  // 是否审批应用用户
  boolean isAppApprovalUser(String appcode, User logged_user);

}
