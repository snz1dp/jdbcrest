package com.snz1.jdbc.rest.api;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.snz1.jdbc.rest.RunConfig;
import com.snz1.jdbc.rest.Version;
import com.snz1.jdbc.rest.service.AppInfoResolver;
import com.snz1.utils.ContextUtils;
import com.snz1.utils.WebUtils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.MediaType;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.view.RedirectView;

import gateway.api.Return;
import gateway.sc.v2.FunctionTreeNode;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@RestController
@Api(tags = "0、应用信息")
@RequestMapping
public class AppPublishApi {

  private FunctionTreeNode[] EMPTY_TREENODES = new FunctionTreeNode[0];

  @Autowired
  private RunConfig runConfig;

  @Resource
  private AppInfoResolver appInfoResolver;

  private ModelAndView rootRedirecrt(HttpServletRequest request, HttpServletResponse response) {
    ModelAndView mav = new ModelAndView();
    String dash_url = WebUtils.getPublishURLViaGateway(request, runConfig.getDefaultTargetUrl());
    mav.setView(new RedirectView(dash_url));
    return mav;
  }

	@ApiOperation(value="默认页面跳转", hidden = true)
	@GetMapping(path = "/index", produces = MediaType.TEXT_HTML_VALUE)
	public ModelAndView handleDefaultPage(
    HttpServletRequest request,
    HttpServletResponse response
  ) {
    return rootRedirecrt(request, response);
	}

	@ApiOperation(value="目录默认跳转", hidden = true)
	@GetMapping(path = "", produces = MediaType.TEXT_HTML_VALUE)
	public ModelAndView handleDessfaultPage(
    HttpServletRequest request,
    HttpServletResponse response
  ) {
    return rootRedirecrt(request, response);
	}

  @ApiOperation("应用版本信息")
  @GetMapping(path = "/version")
  public Return<Version> getAppVersion() {
    Version appVersion = appInfoResolver.getVersion();
    return Return.wrap(appVersion);
  }

  @ApiOperation("获取请求头信息")
	@GetMapping(path = "/headers")
  public Return<Map<String, String>> get_request_headers() {
    HttpServletRequest request = ContextUtils.getHttpServletRequest();
    Enumeration<String> header_names = request.getHeaderNames();
    Map<String, String> header_map = new HashMap<>();
    while(header_names.hasMoreElements()) {
      String header_name = header_names.nextElement();
      header_map.put(header_name, request.getHeader(header_name));
    }
    return Return.wrap(header_map);
  }


  @GetMapping(path = "/functions")
  @ApiOperation("系统功能树")
  @PreAuthorize("isAuthenticated")
  @ConditionalOnProperty(prefix = "spring.security", name = "ssoheader", havingValue = "true", matchIfMissing = false)
  public Return<FunctionTreeNode[]> functions() {
    if (runConfig.hasPermissionDefinition() &&
      runConfig.getPermissionDefinition().getFunctions() != null
    ) {
      FunctionTreeNode[] nodes = runConfig.getPermissionDefinition().getFunctions();
      if (nodes.length != 1) return Return.wrap(nodes);
      return Return.wrap(nodes[0].getChildren().toArray(new FunctionTreeNode[0]));
    } else {
      return Return.wrap(EMPTY_TREENODES);
    }
  }

}
