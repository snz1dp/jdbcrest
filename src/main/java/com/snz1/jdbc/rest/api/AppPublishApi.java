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
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.view.RedirectView;

import gateway.api.Return;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;

@RestController
@Tag(name = "0、应用信息")
@RequestMapping
public class AppPublishApi {

  @Autowired
  private RunConfig runConfig;

  @Resource
  private AppInfoResolver appInfoResolver;

	@Operation(summary = "目录默认跳转", hidden = true)
	@GetMapping(path = "index", produces = MediaType.TEXT_HTML_VALUE)
	public ModelAndView handleDefaultPage(
    HttpServletRequest request,
    HttpServletResponse response
  ) {
    return rootRedirect(request, response);
  }

  @Operation(summary = "目录默认跳转", hidden = true)
	@GetMapping(path = "", produces = MediaType.TEXT_HTML_VALUE)
	public ModelAndView handleDessfaultPage(
    HttpServletRequest request,
    HttpServletResponse response
  ) {
    return rootRedirect(request, response);
	}

  private ModelAndView rootRedirect(
    HttpServletRequest request,
    HttpServletResponse response
  ) {
    ModelAndView mav = new ModelAndView();
    String dash_url = WebUtils.getPublishURLViaGateway(request, runConfig.getDefaultTargetUrl());
    mav.setView(new RedirectView(dash_url));
    return mav;
  }

  @Operation(summary = "应用版本信息")
  @GetMapping(path = "/version")
  public Return<Version> getAppVersion() {
    Version appVersion = appInfoResolver.getVersion();
    return Return.wrap(appVersion);
  }

  @Operation(summary = "获取请求头信息")
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

}
