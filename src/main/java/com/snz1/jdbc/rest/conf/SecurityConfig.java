package com.snz1.jdbc.rest.conf;

import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@AutoConfigureBefore
@Configuration("jdbcrest::SecurityConfig")
public class SecurityConfig {

  @Bean
  @ConditionalOnProperty(prefix = "spring.security", name = "ssoheader", havingValue = "false", matchIfMissing = true)
  public org.springframework.security.core.userdetails.UserDetailsService stubUserDetailsService() {
    return new StubUserDetailsService();
  }

  @Bean
  @ConditionalOnMissingBean(com.snz1.jdbc.rest.service.LoggedUserContext.class)
  public com.snz1.jdbc.rest.service.LoggedUserContext loggedUserContext() {
    return new com.snz1.jdbc.rest.service.impl.LoggedUserContextImpl();
  }

  private static class StubUserDetailsService extends com.snz1.web.security.AbstractUserDetailsService {

    @Override
    protected com.snz1.web.security.UserDetails loadUserDetails(int oauth_from, String scope, String user) {
      throw new org.springframework.security.core.userdetails.UsernameNotFoundException(user, null);
    }

  }

}

