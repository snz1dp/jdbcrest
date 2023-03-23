package com.snz1.jdbc.rest.service.impl;

import org.springframework.jdbc.core.JdbcTemplate;

import com.snz1.jdbc.rest.data.JdbcQueryRequest;
import com.snz1.jdbc.rest.data.JdbcQueryStatement;
import com.snz1.jdbc.rest.provider.SQLDialectProvider;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@ToString
@EqualsAndHashCode
public class TotalQueryRequestHandler {

  private JdbcTemplate jdbcTemplate;

  private JdbcQueryRequest request;

  private SQLDialectProvider dialect_provider;

  public TotalQueryRequestHandler(JdbcQueryRequest request, JdbcTemplate jdbcTemplate, SQLDialectProvider sql_dialect_provider) {
    this.jdbcTemplate = jdbcTemplate;
    this.request = request;
    this.dialect_provider = sql_dialect_provider;
  }

  public long execute() {
    JdbcQueryStatement count_query = dialect_provider.prepareQueryCount(this.getRequest());
    Long query_count = 0l;
    if (count_query.hasParameter()) {
      query_count = jdbcTemplate.queryForObject(count_query.getSql(), Long.class, count_query.getParameters().toArray(new Object[0]));
    } else {
      query_count = jdbcTemplate.queryForObject(count_query.getSql(), Long.class);
    }
    return query_count != null ? query_count : 0l;
  }

}

