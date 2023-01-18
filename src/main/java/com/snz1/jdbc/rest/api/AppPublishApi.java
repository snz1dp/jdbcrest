package com.snz1.jdbc.rest.api;

import java.util.Calendar;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.snz1.jdbc.rest.RunConfig;
import com.snz1.jdbc.rest.Version;
import com.snz1.jdbc.rest.service.AppInfoResolver;
import com.snz1.jdbc.rest.stats.CacheStatisticsCollector;
import com.snz1.utils.CalendarUtils;
import com.snz1.utils.ContextUtils;
import com.snz1.utils.TimeZoneUtils;
import com.snz1.utils.WebUtils;

import org.apache.commons.lang3.StringUtils;
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
import gateway.sc.v2.RoleGroup;
import gateway.sc.v2.config.CacheStatistics;
import gateway.sc.v2.config.LicenseSupport;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;

@RestController
@Tag(name = "应用信息")
@RequestMapping
public class AppPublishApi {

  private FunctionTreeNode[] EMPTY_TREENODES = new FunctionTreeNode[0];
  private RoleGroup[] EMPTY_GROUPS = new RoleGroup[0];

  @Resource
  private RunConfig runConfig;

  @Resource
  private AppInfoResolver appInfoResolver;

  @Resource
  private CacheStatisticsCollector cacheStatisticsCollector;

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
    String root_url = WebUtils.getPublishURLViaGateway(request, runConfig.getDefaultTargetUrl());
    mav.setView(new RedirectView(root_url));
    return mav;
  }

  @Operation(summary = "服务版本信息")
  @GetMapping(path = "/version")
  public Return<Map<String, Object>> getAppVersion() {
    Map<String, Object> ret = new HashMap<>(2);
    Version appVersion = appInfoResolver.getVersion();
    Map<String, Object> version_data = new LinkedHashMap<>();
    ret.put("app", version_data);

    version_data.put("product_code", appVersion.getProduct_code());
    version_data.put("product_name", appVersion.getProduct_name());
    version_data.put("company_name", appVersion.getCompany_name());
    version_data.put("company_url", appVersion.getCompany_url());
    version_data.put("contact_email", appVersion.getContact_email());
    version_data.put("legal_copyright", appVersion.getLegal_copyright());
    version_data.put("product_version", appVersion.getProduct_version());
    if (StringUtils.isNotBlank(runConfig.getService_name())) {
      version_data.put("service_name", runConfig.getService_name());
    } else {
      version_data.put("service_name", appVersion.getProduct_name());
    }
    version_data.put("service_version", runConfig.getService_version());

    LicenseSupport license_support = appInfoResolver.getLicenseSupport();
    Date first_run_time  = runConfig.getFirstRunTime();
    if (license_support != null) {
      ret.put("license", license_support);
      if (license_support.getPrebationary() != null) {
        Date end_time = CalendarUtils.add(first_run_time, TimeZoneUtils.getCurrent(), Calendar.DATE, license_support.getPrebationary());
        ret.put("end", end_time);
      } else if (StringUtils.isBlank(license_support.getProduct_name())) {
        Date end_time = CalendarUtils.add(first_run_time, TimeZoneUtils.getCurrent(), Calendar.MONTH, 3);
        ret.put("end", end_time);
      } else {
        version_data.put("service_name", license_support.getProduct_name());
      }
    } else {
      Date end_time = CalendarUtils.add(first_run_time, TimeZoneUtils.getCurrent(), Calendar.MONTH, 3);
      ret.put("end", end_time);
    }
    return Return.wrap(ret);
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

  @GetMapping(path = "/functions")
  @Operation(summary = "系统功能树")
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

  @GetMapping(path = "/groups")
  @Operation(summary = "权限分组定义")
  @PreAuthorize("isAuthenticated")
  @ConditionalOnProperty(prefix = "spring.security", name = "ssoheader", havingValue = "true", matchIfMissing = false)
  public Return<RoleGroup[]> groups() {
    if (runConfig.hasPermissionDefinition() &&
      runConfig.getPermissionDefinition().getGroups() != null
    ) {
      RoleGroup[] nodes = runConfig.getPermissionDefinition().getGroups();
      return Return.wrap(nodes);
    } else {
      return Return.wrap(EMPTY_GROUPS);
    }
  }

  @GetMapping(path = "/cache/statistics")
  @Operation(summary = "缓存统计数据")
  @PreAuthorize("isAuthenticated")
  @ConditionalOnProperty(prefix = "spring.security", name = "ssoheader", havingValue = "true", matchIfMissing = false)
  public Return<CacheStatistics> cacheStatistics() {
    return Return.wrap(cacheStatisticsCollector.getCacheStatistics());
  }

}
