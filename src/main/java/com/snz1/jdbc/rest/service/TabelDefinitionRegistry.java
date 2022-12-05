package com.snz1.jdbc.rest.service;

import com.snz1.jdbc.rest.data.TableDefinition;

public interface TabelDefinitionRegistry {

  TableDefinition getTableDefinition(String table_name);

  boolean hasTableDefinition(String table_name);

}
