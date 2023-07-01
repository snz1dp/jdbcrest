package com.snz1.jdbc.rest.service;

import java.util.Collection;

import com.snz1.jdbc.rest.data.SQLServiceDefinition;

// SQL服务注册表
public interface SQLServiceRegistry {

  SQLServiceDefinition getService(String service_path);

  Collection<SQLServiceDefinition> getServices();

}
