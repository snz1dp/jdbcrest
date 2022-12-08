package com.snz1.jdbc.rest.service;

import com.snz1.jdbc.rest.data.SQLServiceDefinition;

public interface SQLServiceRegistry {

  SQLServiceDefinition getService(String service_path);
    
}
