package com.snz1.jdbc.rest.conf;

import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;

import com.snz1.web.security.AbstractUserDetailsService;
import com.snz1.web.security.UserDetails;

@AutoConfigureBefore
@Configuration
public class SecurityConfig {

  @Bean
  @ConditionalOnProperty(prefix = "spring.security", name = "ssoheader", havingValue = "false", matchIfMissing = true)
  public UserDetailsService stubUserDetailsService() {
    return new StubUserDetailsService();
  }

  private static class StubUserDetailsService extends AbstractUserDetailsService {

    @Override
    protected UserDetails loadUserDetails(int oauth_from, String scope, String user) {
      throw new UsernameNotFoundException(user, null);
    }

  }

}
