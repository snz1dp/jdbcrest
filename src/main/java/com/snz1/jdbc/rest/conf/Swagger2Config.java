package com.snz1.jdbc.rest.conf;

import com.snz1.jdbc.rest.Version;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 通过@Configuration注解，让Spring来加载该类配置。再通过@EnableSwagger2注解来启用Swagger2。
 * 再通过createRestApi函数创建Docket的Bean之后， apiInfo()用来创建该Api的基本信息（这些基本信息会展现在文档页面中）。
 * select()函数返回一个ApiSelectorBuilder实例用来控制哪些接口暴露给Swagger来展现，
 * 本例采用指定扫描的包路径来定义，Swagger会扫描该包下所有Controller定义的API， 并产生文档内容（除了被@ApiIgnore指定的请求）。
 *
 * 添加文档内容
 *
 * 在完成了上述配置后，其实已经可以生产文档内容，但是这样的文档主要针对请求本身，
 * 而描述主要来源于函数等命名产生，对用户并不友好，我们通常需要自己增加一些说明来丰富文档内容。
 * 如:我们通过@ApiOperation注解来给API增加说明、
 * 通过@ApiImplicitParams、@ApiImplicitParam注解来给参数增加说明。
 *
 */
@Configuration
public class Swagger2Config {

  @Autowired
	private Version version;

	@Bean
	public OpenAPI openApi() {
			OpenAPI openAPI = new OpenAPI();
			// 基本信息
			Contact contact = new Contact();
			contact.setName(version.getCompany_name());
			contact.setEmail(version.getContact_email());
			contact.setUrl(version.getCompany_url());
			openAPI.info(new Info().title(version.getProduct_name())
							.description(version.getDescription())
							.version(version.getProduct_version())
							.contact(contact)
			);
			return openAPI;
	}

}
