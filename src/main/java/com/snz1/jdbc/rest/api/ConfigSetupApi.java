package com.snz1.jdbc.rest.api;

import javax.annotation.Resource;

import org.apache.commons.lang3.Validate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.apache.commons.lang3.StringUtils;

import com.snz1.jdbc.rest.Constants;
import com.snz1.jdbc.rest.RunConfig;
import com.snz1.utils.Configurer;
import com.snz1.jdbc.rest.service.AppInfoResolver;

import gateway.api.Return;
import gateway.sc.v2.ToolProvider;
import gateway.sc.v2.config.LicenseSupport;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;

@RestController
@Tag(name = "系统配置")
@RequestMapping
public class ConfigSetupApi {

  @Resource
  private ToolProvider toolProvider;

  @Resource
  private RunConfig runConfig;

  @Resource
  private AppInfoResolver appInfoResolver;

  @Operation(summary = "设置产品授权代码")
	@PostMapping(path = "/config/license")
	public Return<LicenseSupport> update_product_license(
		@Parameter(description = "授权代码")
		@RequestParam("license")
		String license
	) {
    Validate.isTrue(runConfig.isPersistenceConfig(), "当前运行配置不支持动态设置授权代码");
    LicenseSupport support = toolProvider.decodeLicense(license);
    if (runConfig.getStrict_license()) {
      if (!StringUtils.equals(support.getProduct_code(), runConfig.getApplicationCode())) {
        throw new IllegalStateException("无效的服务授权代码");
      }
      if (!StringUtils.equals(support.getDeployment_id(), appInfoResolver.getDeploymentId())) {
        throw new IllegalStateException("无效的服务授权代码");
      }
    }
    Configurer.setAppProperty(Constants.LICENSE_CODE_ARG, license);
		return Return.wrap(support);
	}

}
