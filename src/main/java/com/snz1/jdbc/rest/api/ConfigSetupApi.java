package com.snz1.jdbc.rest.api;

import javax.annotation.Resource;

import org.apache.commons.lang3.Validate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.snz1.jdbc.rest.Constants;
import com.snz1.jdbc.rest.RunConfig;
import com.snz1.utils.Configurer;

import gateway.api.Return;
import gateway.sc.v2.ToolProvider;
import gateway.sc.v2.config.LicenseSupport;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;

@RestController
@Tag(name = "9、系统配置")
@RequestMapping
public class ConfigSetupApi {

  @Resource
  private ToolProvider toolProvider;

  @Resource
  private RunConfig runConfig;

  @Operation(summary = "设置产品授权代码")
	@PostMapping(path = "/config/license")
	public Return<LicenseSupport> update_product_license(
		@Parameter(description = "授权代码")
		@RequestParam("license")
		String license
	) {
    Validate.isTrue(runConfig.isPersistenceConfig(), "当前运行配置不支持动态设置授权代码");
    LicenseSupport support = toolProvider.decodeLicense(license);
    Configurer.setAppProperty(Constants.LICENSE_CODE_ARG, license);
		return Return.wrap(support);
	}

}
