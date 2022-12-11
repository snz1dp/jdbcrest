package com.snz1.jdbc.rest.service;

import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.snz1.web.security.User;

import gateway.sc.v2.User.IdType;

public interface LoggedUserContext {

  /**
   * 获得当前登录用户对象
   *
   * @return {@link User}
   */
  User getLoggedUser();

  // 用户是否登录
  boolean isUserLogged();

  /**
   * 获取已登录用户信息
   *
   * @return {@link UserInfo}
   */
  UserInfo getLoginUserInfo();

  UserInfo getLoginUserInfo(boolean load_roles);

  // 是否有权限
  boolean hasRole(String role);

  // 是否有任何权限
  boolean hasAnyRole(String ...roles);

  public class UserInfo implements User, Serializable {

    private static final long serialVersionUID = 6161077033448193041L;

    @JsonIgnore
    private gateway.sc.v2.User proxy;

    private String auth_mode;

    private String login_scope;

    private List<String> roles;

    public UserInfo(gateway.sc.v2.User user, String auth_mode, String login_scope, List<String> roles) {
      this.proxy = user;
      this.roles = roles;
    }

    public String getJianpin() {
      return proxy.getJianpin();
    }

    public String getQuanpin() {
      return proxy.getQuanpin();
    }

    public void setCode(String code) {
      proxy.setCode(code);
    }

    public void setEnabled(Boolean enabled) {
      proxy.setEnabled(enabled);
    }

    public void setJianpin(String jianpin) {
      proxy.setJianpin(jianpin);
    }

    public void setQuanpin(String quanpin) {
      proxy.setQuanpin(quanpin);
    }

    public Date getCreate_time() {
      return proxy.getCreate_time();
    }

    public String getMark() {
      return proxy.getMark();
    }

    public Date getModify_time() {
      return proxy.getModify_time();
    }

    public void setEmployeeid(String employeeid) {
      proxy.setEmployeeid(employeeid);
    }

    public void setIdcard(String idcard) {
      proxy.setIdcard(idcard);
    }

    public void setMark(String mark) {
      proxy.setMark(mark);
    }

    public void setModify_time(Date modify_time) {
      proxy.setModify_time(modify_time);
    }

    public void setOffice(String office) {
      proxy.setOffice(office);
    }

    public void setOwner_place(String owner_place) {
      proxy.setOwner_place(owner_place);
    }

    public void setRegist_email(String regist_email) {
      proxy.setRegist_email(regist_email);
    }

    public void setRegist_mobile(String regist_mobile) {
      proxy.setRegist_mobile(regist_mobile);
    }

    public void setTelephone(String telephone) {
      proxy.setTelephone(telephone);
    }

    public String getUser_id() {
      return proxy.getUser_id();
    }

    public void setUser_id(String user_id) {
      proxy.setUser_id(user_id);
    }

    public String getCreate_user() {
      return proxy.getCreate_user();
    }

    public String getModify_user() {
      return proxy.getModify_user();
    }

    public String getUser_scope() {
      return proxy.getUser_scope();
    }

    public void setCreate_user(String create_user) {
      proxy.setCreate_user(create_user);
    }

    public void setModify_user(String modify_user) {
      proxy.setModify_user(modify_user);
    }

    public void setUser_scope(String user_scope) {
      proxy.setUser_scope(user_scope);
    }

    @Override
    @JsonIgnore
    public String getUserid() {
      return proxy.getUser_id();
    }

    @Override
    public String getAccount_name() {
      return proxy.getUser_name();
    }

    @Override
    public String getDisplay_name() {
      return proxy.getName();
    }

    public String getLogin_scope() {
      return login_scope;
    }

    @Override
    public String getScope() {
      return proxy.getUser_scope();
    }

    @Override
    public String getAuth_mode() {
      return auth_mode;
    }

    @Override
    public String getRegist_email() {
      return proxy.getRegist_email();
    }

    @Override
    public String getRegist_mobile() {
      return proxy.getRegist_mobile();
    }

    public String getIdcard() {
      return proxy.getIdcard();
    }

    public String getOffice() {
      return proxy.getOffice();
    }

    public String getTelephone() {
      return proxy.getTelephone();
    }

    public Date getBirthday() {
      return proxy.getBirthday();
    }

    public String getCode() {
      return proxy.getCode();
    }

    public String getUser_name() {
      return proxy.getUser_name();
    }

    public String getOwner_place() {
      return proxy.getOwner_place();
    }

    public Integer getSort_order() {
      return proxy.getSort_order();
    }

    public String getUsername() {
      return proxy.getUsername();
    }

    @Override
    public List<String> getRoles() {
      return this.roles;
    }

    public void setRoles(List<String> roles) {
      this.roles = roles;
    }

    public Boolean getEnabled () {
      return proxy.getEnabled();
    }

    public Map<String, Object> getExtends_properties() {
      return proxy.getExtends_properties();
    }

    public String getEmployeeid() {
      return proxy.getEmployeeid();
    }

    public String getName() {
      return proxy.getName();
    }

    public void setUser_name(String user_name) {
      proxy.setUser_name(user_name);
    }

    public void setUsername(String val) {
      proxy.setUsername(val);
    }

    public void setName(String val) {
      proxy.setName(val);
    }

    public String getIdByType(IdType idtype) {
      if (Objects.equals(IdType.id, idtype)) {
        return this.getUser_id();
      } else if (Objects.equals(IdType.uname, idtype)) {
        return this.getUser_name();
      } else if (Objects.equals(IdType.email, idtype)) {
        return this.getRegist_email();
      } else if (Objects.equals(IdType.mobi, idtype)) {
        return this.getRegist_mobile();
      } else if (Objects.equals(IdType.code, idtype)) {
        return this.getCode();
      } else if (Objects.equals(IdType.idcard, idtype)) {
        return this.getIdcard();
      } else if (Objects.equals(IdType.empid, idtype)) {
        return this.getEmployeeid();
      }
      return this.getUser_name();
    }

  }

}
