package org.openapitools.codegen.postgresql;

import org.openapitools.codegen.AbstractOptionsTest;
import org.openapitools.codegen.CodegenConfig;
import org.openapitools.codegen.languages.PostgresqlSchemaCodegen;
import org.openapitools.codegen.options.PostgresqlSchemaOptionsProvider;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class PostgresqlSchemaOptionsTest extends AbstractOptionsTest {
    private PostgresqlSchemaCodegen clientCodegen = mock(PostgresqlSchemaCodegen.class, mockSettings);

    public PostgresqlSchemaOptionsTest() {
        super(new PostgresqlSchemaOptionsProvider());
    }

    @Override
    protected CodegenConfig getCodegenConfig() {
        return clientCodegen;
    }

    @SuppressWarnings("unused")
    @Override
    protected void verifyOptions() {
        verify(clientCodegen).setDefaultDatabaseName(PostgresqlSchemaOptionsProvider.DEFAULT_DATABASE_NAME_VALUE);
        verify(clientCodegen).setJsonDataTypeEnabled(Boolean.valueOf(PostgresqlSchemaOptionsProvider.JSON_DATA_TYPE_ENABLED_VALUE));
        verify(clientCodegen).setIdentifierNamingConvention(PostgresqlSchemaOptionsProvider.IDENTIFIER_NAMING_CONVENTION_VALUE);
        verify(clientCodegen).setNamedParametersEnabled(Boolean.valueOf(PostgresqlSchemaOptionsProvider.NAMED_PARAMETERS_ENABLED_VALUE));
    }
}
