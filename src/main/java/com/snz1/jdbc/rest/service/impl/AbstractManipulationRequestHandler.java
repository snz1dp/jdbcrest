package com.snz1.jdbc.rest.service.impl;

import com.snz1.jdbc.rest.data.ManipulationRequest;
import com.snz1.jdbc.rest.service.AppInfoResolver;
import com.snz1.jdbc.rest.service.JdbcTypeConverterFactory;
import com.snz1.jdbc.rest.service.LoggedUserContext;
import com.snz1.jdbc.rest.service.SQLDialectProvider;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public abstract class AbstractManipulationRequestHandler<T> extends AbstractRequestHandler<T> {

  private ManipulationRequest request;

  public AbstractManipulationRequestHandler(
    ManipulationRequest request,
    SQLDialectProvider sql_dialect_provider,
    JdbcTypeConverterFactory type_converter_factory,
    LoggedUserContext loggedUserContext,
    AppInfoResolver appInfoResolver
  ) {
    super(sql_dialect_provider, type_converter_factory, loggedUserContext, appInfoResolver);
    this.request = request;
  }

}
