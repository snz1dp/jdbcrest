package com.snz1.jdbc.rest.api;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.io.ClassPathResource;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import com.snz1.web.security.User;
import gateway.api.Page;
import gateway.api.Return;
import gateway.sc.v2.FunctionManager;
import gateway.sc.v2.FunctionNode;
import gateway.sc.v2.FunctionTreeNode;
import gateway.sc.v2.UserManager;
import gateway.sc.v2.User.IdType;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;

import com.snz1.jdbc.rest.RunConfig;
import com.snz1.jdbc.rest.service.AppInfoResolver;
import com.snz1.jdbc.rest.service.LoggedUserContext;
import com.snz1.jdbc.rest.service.LoggedUserContext.UserInfo;
import lombok.extern.slf4j.Slf4j;

@Tag(name = "7、用户帐户")
@RestController
@Slf4j
@ConditionalOnProperty(prefix = "spring.security", name = "ssoheader", havingValue = "true", matchIfMissing = false)
public class UserProfileApi {

  @Autowired
  private LoggedUserContext loggedUserContext;

  @Autowired
  private UserManager userManager;

  @Autowired
  private FunctionManager functionManager;

  @Autowired
  private AppInfoResolver appInfoResolver;

  @Autowired
  private RunConfig runConfig;

  private byte[] defaultUserHeadImage;

  private void loadDefaultUserImage() {
    ClassPathResource resource = new ClassPathResource("data/user.png");
    try {
      this.defaultUserHeadImage = IOUtils.toByteArray(resource.getInputStream());
    } catch (IOException e) {
      throw new IllegalStateException("无法加载用户缺省头像", e);
    }
  }

  public UserProfileApi () {
    this.loadDefaultUserImage();
  }

  @GetMapping(path = "/user/headimg")
  @Operation(summary = "当前用户头像")
  @PreAuthorize("isAuthenticated()")
  public byte[] user_headimg() {
    UserInfo user = loggedUserContext.getLoginUserInfo();
    if (user.getExtends_properties() == null || !user.getExtends_properties().containsKey("head_img")) {
      return this.defaultUserHeadImage;
    }
    String head_img_datauri = null;
    try {
      head_img_datauri = (String)user.getExtends_properties().get("head_img");
      if (head_img_datauri.startsWith("data:image/jpeg")) {
        head_img_datauri = head_img_datauri.substring("data:image/jpeg;base64,".length());
      } else if (head_img_datauri.startsWith("data:image/png")) {
        head_img_datauri = head_img_datauri.substring("data:image/png;base64,".length());
      } else if (head_img_datauri.startsWith("data:image/gif")) {
        head_img_datauri = head_img_datauri.substring("data:image/gif;base64,".length());
      } else {
        throw new IllegalStateException("用户头像图片格式错误");
      }
      return Base64.decodeBase64(head_img_datauri);
    } catch(Throwable e) {
      if (log.isDebugEnabled()) {
        log.debug("错误内容: " + head_img_datauri);
      }
      return this.defaultUserHeadImage;
    }
  }

  @GetMapping(path = "/user/profile")
  @Operation(summary = "当前用户信息")
  @PreAuthorize("isAuthenticated()")
  public Return<UserInfo> userinfo() {
    UserInfo userinfo = loggedUserContext.getLoginUserInfo(false);
		Set<String> roles = new HashSet<>(userManager.getUserRoles(
      userinfo.getUserid(), IdType.id, null, true, runConfig.getApplicationCode()
    ));
    userinfo.setRoles(new LinkedList<>(roles));
    return Return.wrap(userinfo);
  }

  @Operation(summary = "修改人员帐号")
	@PostMapping("/user/profile")
  @PreAuthorize("isAuthenticated()")
	public Return<UserInfo> updateUser(
    @Parameter(description = "确认密码")
    @RequestParam(value = "verify_password", required = false)
    String verify_password,
    @Parameter(description = "新的密码")
    @RequestParam(value = "new_password", required = false)
    String new_password,
    @Parameter(description = "用户密码")
    @RequestParam(value = "password", required = false)
    String password,
		@Parameter(description = "帐号配置")
		@RequestBody
		gateway.sc.v2.User user
	) {
    UserInfo ouser = loggedUserContext.getLoginUserInfo(false);

    Validate.isTrue(
      userManager.verifyUserPassword(
        user.getUserid(), IdType.id, password
      ),
      "用户密码不正确"
    );

    if (StringUtils.isNotBlank(new_password) && !StringUtils.equals(verify_password, new_password)) {
      throw new IllegalArgumentException("新的密码与确认密码不一致");
    }

    gateway.sc.v2.User existed = userManager.getUserById(ouser.getUser_id());
    Validate.notNull(existed, "用户数据不存在");

    existed.setName(user.getName());
    existed.setRegist_mobile(user.getRegist_mobile());
    existed.setRegist_email(user.getRegist_email());

		user = userManager.updateUser(existed);

    if (StringUtils.isNotBlank(new_password)) {
      userManager.updateUserPassword(user.getUser_id(), IdType.id, new_password);
    }

		return Return.wrap(loggedUserContext.getLoginUserInfo());
	}

  @Operation(summary = "获取人员权限角色")
	@GetMapping("/user/roles")
  @PreAuthorize("isAuthenticated()")
	public Return<List<String>> getUserRole(//
		@Parameter(description = "组织域代码（缺省为默认）")
		@RequestParam(value = "version", required = false)
		String version,
		@Parameter(description = "是否包含职位角色")
		@RequestParam(value = "contain_positions_roles", required = false)
		Boolean contain_positions_roles,
		@Parameter(description = "应用ID")
		@RequestParam(value = "appid", required = false)
		String appid
	) {
    if (StringUtils.isBlank(appid)) {
      appid = runConfig.getApplicationCode();
    }
    User user = loggedUserContext.getLoggedUser();
		Set<String> roles = new HashSet<>(userManager.getUserRoles(
      user.getUserid(), IdType.id, version, contain_positions_roles, appid
    ));
    return Return.wrap(new LinkedList<>(roles));
	}

  @Operation(summary = "当前用户拥有权限的功能模块代码")
  @GetMapping(path = "/user/functions")
  @PreAuthorize("isAuthenticated()")
  public Return<List<String>> getUserFuntionCodes() {
    User user = loggedUserContext.getLoggedUser();
    Set<String> roles = new HashSet<>(userManager.getUserRoles(user.getUserid(), IdType.id, false, appInfoResolver.getAppId()));
    List<String> function_codes = new LinkedList<>();

    if (this.runConfig.hasPermissionDefinition() &&
      this.runConfig.getPermissionDefinition().getFunctions() != null
    ) {
      for (FunctionTreeNode node : this.runConfig.getPermissionDefinition().getFunctions()) {
        Page<FunctionNode> node_page = null;
        int offset = 0;
        do {
          node_page = functionManager.getFunctionNodes(node.getCode(), null, FunctionNode.Type.all, false, false, true, offset, 100);
          if (node_page.data == null || node_page.data.size() == 0) break;
          for (FunctionNode tn : node_page.data) {
            if (StringUtils.isBlank(tn.getRolecode()) || roles.contains(tn.getRolecode())) {
              function_codes.add(tn.getCode());
            }
          }
          offset += 100;
        } while(offset >= node_page.total);
      }
    }
    return Return.wrap(function_codes);
  }

}
