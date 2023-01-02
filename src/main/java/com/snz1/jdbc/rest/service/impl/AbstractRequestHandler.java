package com.snz1.jdbc.rest.service.impl;

import org.springframework.jdbc.core.ConnectionCallback;

import com.snz1.jdbc.rest.service.AppInfoResolver;
import com.snz1.jdbc.rest.service.JdbcTypeConverterFactory;
import com.snz1.jdbc.rest.service.LoggedUserContext;
import com.snz1.jdbc.rest.service.SQLDialectProvider;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public abstract class AbstractRequestHandler<T> implements ConnectionCallback<T>{

  private SQLDialectProvider sqlDialectProvider;
  
  private JdbcTypeConverterFactory converterFactory;

  private LoggedUserContext loggedUserContext;

  private AppInfoResolver appInfoResolver;
    
}
