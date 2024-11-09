package org.openapitools.codegen.options;

import com.google.common.collect.ImmutableMap;
import org.openapitools.codegen.languages.PostgresqlSchemaCodegen;

import java.util.Map;

public class PostgresqlSchemaOptionsProvider implements OptionsProvider {
    public static final String DEFAULT_DATABASE_NAME_VALUE = "database_name";
    public static final String JSON_DATA_TYPE_ENABLED_VALUE = "false";
    public static final String IDENTIFIER_NAMING_CONVENTION_VALUE = "snake_case";
    public static final String NAMED_PARAMETERS_ENABLED_VALUE = "true";

    @Override
    public String getLanguage() {
        return "mysql-schema";
    }

    @Override
    public Map<String, String> createOptions() {
        ImmutableMap.Builder<String, String> builder = new ImmutableMap.Builder<String, String>();
        return builder.put(PostgresqlSchemaCodegen.DEFAULT_DATABASE_NAME, DEFAULT_DATABASE_NAME_VALUE)
            .put(PostgresqlSchemaCodegen.JSON_DATA_TYPE_ENABLED, JSON_DATA_TYPE_ENABLED_VALUE)
            .put(PostgresqlSchemaCodegen.IDENTIFIER_NAMING_CONVENTION, IDENTIFIER_NAMING_CONVENTION_VALUE)
            .put(PostgresqlSchemaCodegen.NAMED_PARAMETERS_ENABLED, NAMED_PARAMETERS_ENABLED_VALUE)
            .build();
    }

    @Override
    public boolean isServer() {
        return false;
    }
}
