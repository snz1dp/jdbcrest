package com.snz1.jdbc.rest.api;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.MediaType;
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

@Tag(name = "用户帐户")
@RestController
@ConditionalOnProperty(prefix = "spring.security", name = "ssoheader", havingValue = "true", matchIfMissing = false)
public class UserProfileApi {

  private static final String HEAD_IMAGE_ARG = "head_img";

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

  public UserProfileApi () {
  }

  @Operation(summary = "获得当前登录用户头像")
  @GetMapping(path = "/user/headimg")
  public void userHeadimg(HttpServletResponse response) throws IOException {
		User logged_user = loggedUserContext.getLoggedUser();
    gateway.sc.v2.User user_data = userManager.getUserById(logged_user.getUserid());
    Map<String, Object> user_exts = user_data.getExtends_properties();
    if (user_exts == null || !user_exts.containsKey("head_img")) {
      renderDefaultUserImage(response);
    } else {
      ImageStream image_stream = null;
      try {
        image_stream = decodeDataURIAsImage((String)user_exts.get("head_img"));
        if (StringUtils.equals("error", image_stream.contentType)) {
          renderDefaultUserImage(response);
        } else if (StringUtils.equals("redirect", image_stream.contentType)) {
          response.sendRedirect(IOUtils.toString(image_stream.inputStream, "UTF-8"));
        } else {
          response.setContentType(image_stream.contentType);
          try {
            IOUtils.copy(image_stream.inputStream, response.getOutputStream());
          } finally {
            IOUtils.closeQuietly(image_stream.inputStream);
          }
          response.getOutputStream().flush();
        }
      } finally {
        if (image_stream != null && image_stream.inputStream != null) {
          IOUtils.closeQuietly(image_stream.inputStream);
        }
      }
		}
  }

  @GetMapping(path = "/user/profile")
  @Operation(summary = "当前用户信息")
  @PreAuthorize("isAuthenticated()")
  public Return<gateway.sc.v2.User> userinfo() {
    User logged_user = loggedUserContext.getLoggedUser();
    gateway.sc.v2.User user_data = userManager.getUserById(logged_user.getUserid());
		Set<String> roles = new HashSet<>(logged_user.getRoles());
    user_data.setRoles(new LinkedList<>(roles));
    return Return.wrap(user_data);
  }

  @Operation(description = "修改人员帐号")
	@PostMapping("/user/profile")
  @PreAuthorize("isAuthenticated()")
	public Return<gateway.sc.v2.User> updateUser(
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
    User ouser = loggedUserContext.getLoggedUser();

    Validate.isTrue(
      userManager.verifyUserPassword(
        user.getUserid(), IdType.id, password
      ),
      "用户密码不正确"
    );

    if (StringUtils.isNotBlank(new_password) && !StringUtils.equals(verify_password, new_password)) {
      throw new IllegalArgumentException("新的密码与确认密码不一致");
    }

    gateway.sc.v2.User existed = userManager.getUserById(ouser.getUserid());
    Validate.notNull(existed, "用户数据不存在");

    existed.setName(user.getName());
    existed.setRegist_mobile(user.getRegist_mobile());
    existed.setRegist_email(user.getRegist_email());

    if (existed.getExtends_properties() == null) {
      existed.setExtends_properties(new HashMap<>());
    }
    if (user.getExtends_properties() != null && user.getExtends_properties().containsKey(HEAD_IMAGE_ARG)) {
      existed.getExtends_properties().put(HEAD_IMAGE_ARG,
        user.getExtends_properties().get(HEAD_IMAGE_ARG));
    } else {
      existed.getExtends_properties().put(HEAD_IMAGE_ARG, "");
    }

		user = userManager.updateUser(existed);

    if (StringUtils.isNotBlank(new_password)) {
      userManager.updateUserPassword(user.getUser_id(), IdType.id, new_password);
    }

		return Return.wrap(user);
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

  private static void renderDefaultUserImage(HttpServletResponse response) throws IOException {
		response.setContentType(MediaType.IMAGE_PNG_VALUE);
		InputStream image_fin = new ClassPathResource("data/user.png").getInputStream();
		try {
			IOUtils.copy(image_fin, response.getOutputStream());
		} finally {
			IOUtils.closeQuietly(image_fin);
		}
		response.getOutputStream().flush();
	}

  private static ImageStream decodeDataURIAsImage(String imagedata) {
    ImageStream image_stream = new ImageStream();
    if (imagedata.startsWith("data:image/jpeg")) {
      image_stream.contentType = MediaType.IMAGE_JPEG_VALUE;
      imagedata = imagedata.substring("data:image/jpeg;base64,".length());
      image_stream.inputStream = new ByteArrayInputStream(Base64.getDecoder().decode(imagedata));
    } else if (imagedata.startsWith("data:image/png")) {
      image_stream.contentType = MediaType.IMAGE_PNG_VALUE;
      imagedata = imagedata.substring("data:image/png;base64,".length());
      image_stream.inputStream = new ByteArrayInputStream(Base64.getDecoder().decode(imagedata));
    } else if (imagedata.startsWith("data:image/gif")) {
      image_stream.contentType = MediaType.IMAGE_GIF_VALUE;
      imagedata = imagedata.substring("data:image/gif;base64,".length());
      image_stream.inputStream = new ByteArrayInputStream(Base64.getDecoder().decode(imagedata));
    } else if (imagedata.startsWith("data:image/svg+xml")) {
      image_stream.contentType = "image/svg+xml";
      imagedata = imagedata.substring("data:image/svg+xml;base64,".length());
      image_stream.inputStream = new ByteArrayInputStream(Base64.getDecoder().decode(imagedata));
    } else if (imagedata.startsWith("http")) {
      image_stream.contentType = "redirect";
      image_stream.inputStream = new ByteArrayInputStream(imagedata.getBytes());
    } else {
      image_stream.contentType = "error";
    }
    return image_stream;
  }

  public static class ImageStream {
    
    public String contentType;

    public InputStream inputStream;

  }

}
