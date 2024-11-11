package org.openapitools.codegen.languages;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.openapitools.codegen.*;
import org.openapitools.codegen.meta.features.*;
import org.openapitools.codegen.model.ModelMap;
import org.openapitools.codegen.model.ModelsMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.openapitools.codegen.utils.StringUtils.underscore;

@SuppressWarnings("unchecked")
public class PostgresqlSchemaCodegen extends DefaultCodegen implements CodegenConfig {
    private final Logger LOGGER = LoggerFactory.getLogger(PostgresqlSchemaCodegen.class);

    public static final String VENDOR_EXTENSION_POSTGRESQL_SCHEMA = "x-postgresql-schema";
    public static final String DEFAULT_DATABASE_NAME = "defaultDatabaseName";
    public static final String JSON_DATA_TYPE_ENABLED = "jsonDataTypeEnabled";
    public static final String IDENTIFIER_NAMING_CONVENTION = "identifierNamingConvention";
    public static final String NAMED_PARAMETERS_ENABLED = "namedParametersEnabled";
    public static final Integer ENUM_MAX_ELEMENTS = 65535;
    public static final Integer IDENTIFIER_MAX_LENGTH = 64;

    protected Vector<String> postgresqlNumericTypes = new Vector<>(Arrays.asList(
            "BIGINT", "BIGSERIAL", "BIT", "BIT VARYING", "DECIMAL", "DOUBLE PRECISION", "INTEGER", "NUMERIC",
            "REAL", "SMALLINT", "SMALLSERIAL", "SERIAL", "MONEY"
    ));

    protected Vector<String> postgresqlDateAndTimeTypes = new Vector<>(Arrays.asList(
            "DATE", "TIME", "TIME WITH TIME ZONE", "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "INTERVAL"
    ));

    protected Vector<String> postgresqlStringTypes = new Vector<>(Arrays.asList(
            "CHARACTER", "CHARACTER VARYING", "VARCHAR", "TEXT", "BYTEA"
    ));

    protected Vector<String> postgresqlSpatialTypes = new Vector<>(Arrays.asList(
            "BOX", "CIRCLE", "LINE", "LSEG", "PATH", "POINT", "POLYGON"
    ));


    /**
     *  Returns default database name for all PostgreSQL queries
     *  This value must be used with backticks only, e.g. `database_name`
     */
    @Getter protected String defaultDatabaseName = "", databaseNamePrefix = "", databaseNameSuffix = "_db";
    protected String tableNamePrefix = "t_", tableNameSuffix = "";
    protected String columnNamePrefix = "", columnNameSuffix = "";
    /**
     *  Whether JSON data type enabled or disabled in all PostgreSQL queries.
     *  JSON data type requires PostgreSQL version 11
     */
    @Getter @Setter
    protected Boolean jsonDataTypeEnabled = true;
    /**
     *  Whether named parameters enabled or disabled in prepared SQLs
     */
    @Getter @Setter
    protected Boolean namedParametersEnabled = false;
    /**
     *  Returns identifier naming convention for table names and column names.
     */
    @Getter protected String identifierNamingConvention = "original";

    public PostgresqlSchemaCodegen() {
        super();

        modifyFeatureSet(features -> features
                .includeDocumentationFeatures(DocumentationFeature.Readme)
                .wireFormatFeatures(EnumSet.noneOf(WireFormatFeature.class))
                .securityFeatures(EnumSet.noneOf(SecurityFeature.class))
                .excludeGlobalFeatures(
                        GlobalFeature.XMLStructureDefinitions,
                        GlobalFeature.Callbacks,
                        GlobalFeature.LinkObjects,
                        GlobalFeature.ParameterStyling
                )
                .excludeSchemaSupportFeatures(
                        SchemaSupportFeature.Polymorphism
                )
                .clientModificationFeatures(EnumSet.noneOf(ClientModificationFeature.class))
        );
        // clear import mapping (from default generator) as postgresql does not use import directives
        importMapping.clear();
        // https://dev.postgresql.com/doc/refman/8.0/en/keywords.html
        setReservedWordsLowerCase(
                Arrays.asList(
                        // SQL reserved words
                        "ABORT", "ABSOLUTE", "ACCESS", "ACTION", "ADD", "ADMIN", "AFTER", "AGGREGATE", "ALL", "ALSO", "ALTER", "ALWAYS", "ANALYSE", "ANALYZE", "AND", "ANY",
                        "ARRAY", "AS", "ASC", "ASSERTION", "ASSIGNMENT", "ASYMMETRIC", "AT", "ATTACH", "ATTRIBUTE", "AUTHORIZATION", "BACKWARD", "BEFORE", "BEGIN", "BETWEEN",
                        "BIGINT", "BINARY", "BIT", "BOOLEAN", "BOTH", "BY", "CACHE", "CALL", "CALLED", "CASCADE", "CASCADED", "CASE", "CAST", "CATALOG", "CHAIN", "CHAR", "CHARACTER",
                        "CHARACTERISTICS", "CHECK", "CHECKPOINT", "CLASS", "CLOSE", "CLUSTER", "COALESCE", "COLLATE", "COLLATION", "COLUMN", "COLUMNS", "COMMENT", "COMMENTS", "COMMIT",
                        "COMMITTED", "CONCURRENTLY", "CONFIGURATION", "CONFLICT", "CONNECTION", "CONSTRAINT", "CONSTRAINTS", "CONTENT", "CONTINUE", "CONVERSION", "COPY", "COST", "CREATE",
                        "CROSS", "CSV", "CUBE", "CURRENT", "CURRENT_CATALOG", "CURRENT_DATE", "CURRENT_ROLE", "CURRENT_SCHEMA", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "CURSOR",
                        "CYCLE", "DATA", "DATABASE", "DAY", "DEALLOCATE", "DEC", "DECIMAL", "DECLARE", "DEFAULT", "DEFAULTS", "DEFERRABLE", "DEFERRED", "DEFINER", "DELETE", "DELIMITER",
                        "DELIMITERS", "DEPENDS", "DESC", "DETACH", "DICTIONARY", "DISABLE", "DISCARD", "DISTINCT", "DO", "DOCUMENT", "DOMAIN", "DOUBLE", "DROP", "EACH", "ELSE", "ENABLE",
                        "ENCODING", "ENCRYPTED", "END", "ENUM", "ESCAPE", "EVENT", "EXCEPT", "EXCLUDE", "EXCLUDING", "EXCLUSIVE", "EXECUTE", "EXISTS", "EXPLAIN", "EXTENSION", "EXTERNAL",
                        "EXTRACT", "FALSE", "FAMILY", "FETCH", "FILTER", "FIRST", "FLOAT", "FOLLOWING", "FOR", "FORCE", "FOREIGN", "FORWARD", "FREEZE", "FROM", "FULL", "FUNCTION", "FUNCTIONS",
                        "GENERATED", "GLOBAL", "GRANT", "GRANTED", "GREATEST", "GROUP", "GROUPING", "HANDLER", "HAVING", "HEADER", "HOLD", "HOUR", "IDENTITY", "IF", "ILIKE", "IMMEDIATE",
                        "IMMUTABLE", "IMPLICIT", "IMPORT", "IN", "INCLUDING", "INCREMENT", "INDEX", "INDEXES", "INHERIT", "INHERITS", "INITIALLY", "INLINE", "INNER", "INOUT", "INPUT",
                        "INSENSITIVE", "INSERT", "INSTEAD", "INT", "INTEGER", "INTERSECT", "INTERVAL", "INTO", "INVOKER", "IS", "ISNULL", "ISOLATION", "JOIN", "KEY", "LABEL", "LANGUAGE",
                        "LARGE", "LAST", "LATERAL", "LEADING", "LEAKPROOF", "LEAST", "LEFT", "LEVEL", "LIKE", "LIMIT", "LISTEN", "LOAD", "LOCAL", "LOCALTIME", "LOCALTIMESTAMP", "LOCATION",
                        "LOCK", "LOCKED", "LOGGED", "MAPPING", "MATCH", "MATERIALIZED", "MAXVALUE", "METHOD", "MINUTE", "MINVALUE", "MODE", "MONTH", "MOVE", "NAME", "NAMES", "NATIONAL",
                        "NATURAL", "NCHAR", "NEW", "NEXT", "NO", "NONE", "NOT", "NOTHING", "NOTIFY", "NOTNULL", "NOWAIT", "NULL", "NULLIF", "NUMERIC", "OBJECT", "OF", "OFF", "OFFSET", "OIDS",
                        "OLD", "ON", "ONLY", "OPERATOR", "OPTION", "OPTIONS", "OR", "ORDER", "ORDINALITY", "OUT", "OUTER", "OVER", "OVERLAPS", "OVERLAY", "OVERRIDING", "OWNED", "OWNER", "PARSER",
                        "PARTIAL", "PARTITION", "PASSING", "PASSWORD", "PLACING", "PLANS", "POLICY", "POSITION", "PRECEDING", "PRECISION", "PREPARE", "PREPARED", "PRESERVE", "PRIMARY", "PRIOR",
                        "PRIVILEGES", "PROCEDURAL", "PROCEDURE", "PROGRAM", "PUBLICATION", "QUOTE", "RANGE", "READ", "REAL", "REASSIGN", "RECHECK", "RECURSIVE", "REF", "REFERENCES", "REFERENCING",
                        "REFRESH", "REINDEX", "RELATIVE", "RELEASE", "RENAME", "REPEATABLE", "REPLACE", "REPLICA", "RESET", "RESTART", "RESTRICT", "RETURNING", "RETURNS", "REVOKE", "RIGHT", "ROLE",
                        "ROLLBACK", "ROLLUP", "ROUTINE", "ROUTINES", "ROW", "ROWS", "RULE", "SAVEPOINT", "SCHEMA", "SCHEMAS", "SCROLL", "SEARCH", "SECOND", "SECURITY", "SELECT", "SEQUENCE", "SEQUENCES",
                        "SERIALIZABLE", "SERVER", "SESSION", "SESSION_USER", "SET", "SETOF", "SHARE", "SHOW", "SIMILAR", "SIMPLE", "SKIP", "SMALLINT", "SNAPSHOT", "SOME", "SQL", "STABLE", "STANDALONE",
                        "START", "STATEMENT", "STATISTICS", "STDIN", "STDOUT", "STORAGE", "STRICT", "STRIP", "SUBSCRIPTION", "SUBSTRING", "SYMMETRIC", "SYSID", "SYSTEM", "TABLE", "TABLES", "TABLESAMPLE",
                        "TABLESPACE", "TEMP", "TEMPLATE", "TEMPORARY", "TEXT", "THEN", "TIES", "TIME", "TIMESTAMP", "TO", "TRAILING", "TRANSACTION", "TRANSFORM", "TREAT", "TRIGGER", "TRIM", "TRUE", "TRUNCATE",
                        "TRUSTED", "TYPE", "TYPES", "UNBOUNDED", "UNCOMMITTED", "UNENCRYPTED", "UNION", "UNIQUE", "UNKNOWN", "UNLISTEN", "UNLOGGED", "UNTIL", "UPDATE", "USER", "USING", "VACUUM", "VALID",
                        "VALIDATE", "VALIDATOR", "VALUE", "VALUES", "VARCHAR", "VARIADIC", "VARYING", "VERBOSE", "VERSION", "VIEW", "VIEWS", "VOLATILE", "WHEN", "WHERE", "WHITESPACE", "WINDOW", "WITH",
                        "WITHIN", "WITHOUT", "WORK", "WRAPPER", "WRITE", "XML", "XMLATTRIBUTES", "XMLCONCAT", "XMLELEMENT", "XMLEXISTS", "XMLFOREST", "XMLNAMESPACES", "XMLPARSE", "XMLPI", "XMLROOT",
                        "XMLSERIALIZE", "YEAR", "YES", "ZONE"
                )
        );

        /**
         *
         */
        // all types can be threaded as primitives except array, object and refs
        languageSpecificPrimitives = new HashSet<>(
                Arrays.asList(
                        "bigint", "bigserial", "bit", "bit varying", "boolean", "box", "bytea", "character", "character varying", "cidr", "circle", "date", "double precision",
                        "inet", "integer", "interval", "json", "jsonb", "line", "lseg", "macaddr", "macaddr8", "money", "numeric", "path", "pg_lsn", "pg_snapshot", "point",
                        "polygon", "real", "smallint", "smallserial", "serial", "text", "time", "time with time zone", "timestamp", "timestamp with time zone",
                        "tsquery", "tsvector", "txid_snapshot", "uuid", "xml"
                )
        );

        // https://www.postgresql.org/docs/current/datatype.html
        typeMapping.put("array", "JSON");
        typeMapping.put("set", "JSON");
        typeMapping.put("map", "JSON");
        typeMapping.put("List", "JSON");
        typeMapping.put("boolean", "BOOLEAN");
        typeMapping.put("string", "VARCHAR");
        typeMapping.put("int", "INTEGER");
        typeMapping.put("byte", "TEXT");
        typeMapping.put("float", "DECIMAL");
        typeMapping.put("number", "DECIMAL");
        typeMapping.put("date", "TIMESTAMP");
        typeMapping.put("Date", "TIMESTAMP");
        typeMapping.put("DateTime", "TIMESTAMP");
        typeMapping.put("long", "BIGINT");
        typeMapping.put("short", "SMALLINT");
        typeMapping.put("char", "TEXT");
        typeMapping.put("double", "DECIMAL");
        typeMapping.put("object", "JSON");
        typeMapping.put("integer", "INTEGER");
        typeMapping.put("ByteArray", "BYTEA");
        typeMapping.put("binary", "BYTEA");
        typeMapping.put("file", "BYTEA");
        typeMapping.put("UUID", "TEXT");
        typeMapping.put("URI", "TEXT");
        typeMapping.put("BigDecimal", "DECIMAL");

        embeddedTemplateDir = templateDir = "postgresql-schema";

        // it seems that cli options from DefaultCodegen are useless here
        cliOptions.clear();
        addOption(DEFAULT_DATABASE_NAME, "Default database name for all PostgreSQL queries", defaultDatabaseName);
        addSwitch(JSON_DATA_TYPE_ENABLED, "Use special JSON PostgreSQL data type for complex model properties. Requires PostgreSQL version 11. Generates TEXT data type when disabled", jsonDataTypeEnabled);
        addSwitch(NAMED_PARAMETERS_ENABLED, "Generates model prepared SQLs with named parameters, eg. :petName. Question mark placeholder used when option disabled.", namedParametersEnabled);

        // we used to snake_case table/column names, let's add this option
        CliOption identifierNamingOpt = new CliOption(IDENTIFIER_NAMING_CONVENTION,
                "Naming convention of PostgreSQL identifiers(table names and column names). This is not related to database name which is defined by " + DEFAULT_DATABASE_NAME + " option");

        identifierNamingOpt.addEnum("original", "Do not transform original names")
                .addEnum("snake_case", "Use snake_case names")
                .setDefault("original");

        cliOptions.add(identifierNamingOpt);
    }

    @Override
    public CodegenType getTag() {
        return CodegenType.SCHEMA;
    }

    @Override
    public String getName() {
        return "postgresql-schema";
    }

    @Override
    public String getHelp() {
        return "Generates a PostgreSQL schema based on the model or schema defined in the OpenAPI specification (v2, v3).";
    }

    @Override
    public void processOpts() {
        super.processOpts();

        if (additionalProperties.containsKey(DEFAULT_DATABASE_NAME)) {
            if (additionalProperties.get(DEFAULT_DATABASE_NAME).equals("")) {
                additionalProperties.remove(DEFAULT_DATABASE_NAME);
            } else {
                this.setDefaultDatabaseName((String) additionalProperties.get(DEFAULT_DATABASE_NAME));
                // default database name may be escaped, need to overwrite additional prop
                additionalProperties.put(DEFAULT_DATABASE_NAME, getDefaultDatabaseName());
            }
        }

        if (additionalProperties.containsKey(JSON_DATA_TYPE_ENABLED)) {
            this.setJsonDataTypeEnabled(Boolean.valueOf(additionalProperties.get(JSON_DATA_TYPE_ENABLED).toString()));
        } else {
            additionalProperties.put(JSON_DATA_TYPE_ENABLED, getJsonDataTypeEnabled());
        }

        if (additionalProperties.containsKey(NAMED_PARAMETERS_ENABLED)) {
            this.setNamedParametersEnabled(Boolean.valueOf(additionalProperties.get(NAMED_PARAMETERS_ENABLED).toString()));
        }

        additionalProperties.put(NAMED_PARAMETERS_ENABLED, getNamedParametersEnabled());

        if (additionalProperties.containsKey(IDENTIFIER_NAMING_CONVENTION)) {
            this.setIdentifierNamingConvention((String) additionalProperties.get(IDENTIFIER_NAMING_CONVENTION));
        }

        // make model src path available in mustache template
        additionalProperties.put("modelSrcPath", "./" + toSrcPath(modelPackage));
        supportingFiles.add(new SupportingFile("postgresql_schema.mustache", "", "postgresql_schema.sql"));
        supportingFiles.add(new SupportingFile("postgresql_schema_common_column.mustache", "", "postgresql_schema_common_column.sql"));
    }

    @Override
    public ModelsMap postProcessModels(ModelsMap objs) {
        objs = super.postProcessModels(objs);

        for (ModelMap mo : objs.getModels()) {
            CodegenModel model = mo.getModel();
            String modelName = model.getName();
            String tableName = this.toTableName(modelName);
            String modelDescription = model.getDescription();
            Map<String, Object> modelVendorExtensions = model.getVendorExtensions();
            Map<String, Object> postgresqlSchema = new HashMap<>();
            Map<String, Object> tableDefinition = new HashMap<>();

            if (this.getIdentifierNamingConvention().equals("snake_case") && !modelName.equals(tableName)) {
                // add original name in table comment
                String commentExtra = "Original model name - " + modelName + ".";
                modelDescription = (modelDescription == null || modelDescription.isEmpty()) ? commentExtra : modelDescription + ". " + commentExtra;
            }

            if (modelVendorExtensions.containsKey(VENDOR_EXTENSION_POSTGRESQL_SCHEMA)) {
                // user already specified schema values
                LOGGER.info("Found vendor extension in '{}' model, autogeneration skipped", modelName);
            } else {
                modelVendorExtensions.put(VENDOR_EXTENSION_POSTGRESQL_SCHEMA, postgresqlSchema);
                postgresqlSchema.put("tableDefinition", tableDefinition);
                tableDefinition.put("tblName", tableName);
                tableDefinition.put("tblComment", modelDescription);
            }
        }

        return objs;
    }

    @Override
    public void postProcessModelProperty(CodegenModel model, CodegenProperty property) {
        switch (property.getDataType().toUpperCase(Locale.ROOT)) {
            case "BOOLEAN":
                processBooleanTypeProperty(model, property);
                break;
            case "SMALLINT":
            case "INTEGER":
            case "BIGINT":
                processIntegerTypeProperty(model, property);
                break;
            case "DECIMAL":
                processDecimalTypeProperty(model, property);
                break;
            case "BYTEA":
            case "TEXT":
            case "VARCHAR":
                processStringTypeProperty(model, property);
                break;
            case "DATE":
            case "TIMESTAMP":
                processDateTypeProperty(model, property);
                break;
            case "JSON":
                processJsonTypeProperty(model, property);
                break;
            default:
                processUnknownTypeProperty(model, property);
        }
    }

    /**
     * Processes each model's property mapped to integer type and adds related vendor extensions
     *
     * @param model    model
     * @param property model's property
     */
    public void processIntegerTypeProperty(CodegenModel model, CodegenProperty property) {
        Map<String, Object> vendorExtensions = property.getVendorExtensions();
        Map<String, Object> postgresqlSchema = new HashMap<>();
        Map<String, Object> columnDefinition = new HashMap<>();
        ArrayList columnDataTypeArguments = new ArrayList();
        String baseName = property.getBaseName();
        String modelName = model.getName();
        String tableName = this.toTableName(modelName);
        columnDefinition.put("tblName", tableName);
        String colName = this.toColumnName(baseName);
        String dataType = property.getDataType();
        String dataFormat = property.getDataFormat();
        String description = property.getDescription();
        String minimum = property.getMinimum();
        String maximum = property.getMaximum();
        boolean exclusiveMinimum = property.getExclusiveMinimum();
        boolean exclusiveMaximum = property.getIExclusiveMaximum();
        String defaultValue = property.getDefaultValue();
        Boolean required = property.getRequired();
        boolean unsigned = false;
        Boolean isUuid = property.isUuid;
        Boolean isEnum = property.isEnum;

        if (vendorExtensions.containsKey(VENDOR_EXTENSION_POSTGRESQL_SCHEMA)) {
            // user already specified schema values
            LOGGER.info("Found vendor extension in '{}' property, autogeneration skipped", baseName);
            return;
        }

        if (this.getIdentifierNamingConvention().equals("snake_case") && !baseName.equals(colName)) {
            // add original name in column comment
            String commentExtra = "Original param name - " + baseName + ".";
            description = (description == null || description.isEmpty()) ? commentExtra : description + ". " + commentExtra;
        }

        vendorExtensions.put(VENDOR_EXTENSION_POSTGRESQL_SCHEMA, postgresqlSchema);
        postgresqlSchema.put("columnDefinition", columnDefinition);
        columnDefinition.put("colName", colName);

        if (Boolean.TRUE.equals(isEnum)) {
            Map<String, Object> allowableValues = property.getAllowableValues();
            List<Object> enumValues = (List<Object>) allowableValues.get("values");
            for (int i = 0; i < enumValues.size(); i++) {
                if (i > ENUM_MAX_ELEMENTS - 1) {
                    LOGGER.warn(
                            "ENUM column can have maximum of {} distinct elements, following value will be skipped: {}",
                            ENUM_MAX_ELEMENTS, (String) enumValues.get(i));
                    break;
                }
                String value = String.valueOf(enumValues.get(i));
                columnDataTypeArguments.add(toCodegenPostgreSQLDataTypeArgument(value));
            }
            columnDefinition.put("colDataType", "ENUM");
            columnDefinition.put("colDataTypeArguments", columnDataTypeArguments);
        } else {
            if ("int64".equals(dataFormat)) {
                columnDefinition.put("colDataType", "BIGINT");
            } else {
                Long min = (minimum != null) ? Long.parseLong(minimum) : null;
                Long max = (maximum != null) ? Long.parseLong(maximum) : null;
                if (exclusiveMinimum && min != null) min += 1;
                if (exclusiveMaximum && max != null) max -= 1;
                if (min != null && min >= 0) {
                    unsigned = true;
                }
                columnDefinition.put("colUnsigned", unsigned);
                columnDefinition.put("colDataType", getPostgreSQLMatchedIntegerDataType(min, max, unsigned));
            }
        }

        if (Boolean.TRUE.equals(required)) {
            columnDefinition.put("colNotNull", true);
        } else {
            columnDefinition.put("colNotNull", false);
            try {
                columnDefinition.put("colDefault", toCodegenPostgreSQLDataTypeDefault(defaultValue, (String) columnDefinition.get("colDataType")));
            } catch (RuntimeException exception) {
                LOGGER.warn(
                        "Property '{}' of model '{}' mapped to PostgreSQL data type which doesn't support default value",
                        baseName, model.getName());
                columnDefinition.put("colDefault", null);
            }
        }

        if (description != null) {
            columnDefinition.put("colComment", description);
        }
    }

    /**
     * Processes each model's property mapped to decimal type and adds related vendor extensions
     *
     * @param model    model
     * @param property model's property
     */
    public void processDecimalTypeProperty(CodegenModel model, CodegenProperty property) {
        Map<String, Object> vendorExtensions = property.getVendorExtensions();
        Map<String, Object> postgresqlSchema = new HashMap<>();
        Map<String, Object> columnDefinition = new HashMap<>();
        ArrayList columnDataTypeArguments = new ArrayList();
        String baseName = property.getBaseName();
        String colName = this.toColumnName(baseName);
        String modelName = model.getName();
        String tableName = this.toTableName(modelName);
        columnDefinition.put("tblName", tableName);
        String dataType = property.getDataType();
        String dataFormat = property.getDataFormat();
        String description = property.getDescription();
        String minimum = property.getMinimum();
        String maximum = property.getMaximum();
        boolean exclusiveMinimum = property.getExclusiveMinimum();
        boolean exclusiveMaximum = property.getIExclusiveMaximum();
        String defaultValue = property.getDefaultValue();
        Boolean required = property.getRequired();
        boolean unsigned = false;
        Boolean isEnum = property.isEnum;

        if (vendorExtensions.containsKey(VENDOR_EXTENSION_POSTGRESQL_SCHEMA)) {
            // user already specified schema values
            LOGGER.info("Found vendor extension in '{}' property, autogeneration skipped", baseName);
            return;
        }

        if (this.getIdentifierNamingConvention().equals("snake_case") && !baseName.equals(colName)) {
            // add original name in column comment
            String commentExtra = "Original param name - " + baseName + ".";
            description = (description == null || description.isEmpty()) ? commentExtra : description + ". " + commentExtra;
        }

        vendorExtensions.put(VENDOR_EXTENSION_POSTGRESQL_SCHEMA, postgresqlSchema);
        postgresqlSchema.put("columnDefinition", columnDefinition);
        columnDefinition.put("colName", colName);

        if (Boolean.TRUE.equals(isEnum)) {
            Map<String, Object> allowableValues = property.getAllowableValues();
            List<Object> enumValues = (List<Object>) allowableValues.get("values");
            for (int i = 0; i < enumValues.size(); i++) {
                if (i > ENUM_MAX_ELEMENTS - 1) {
                    LOGGER.warn(
                            "ENUM column can have maximum of {} distinct elements, following value will be skipped: {}",
                            ENUM_MAX_ELEMENTS, (String) enumValues.get(i));
                    break;
                }
                String value = String.valueOf(enumValues.get(i));
                columnDataTypeArguments.add(toCodegenPostgreSQLDataTypeArgument(value));
            }
            columnDefinition.put("colDataType", "ENUM");
            columnDefinition.put("colDataTypeArguments", columnDataTypeArguments);
        } else {
            Float min = (minimum != null) ? Float.valueOf(minimum) : null;
            Float max = (maximum != null) ? Float.valueOf(maximum) : null;
            if (exclusiveMinimum && min != null) min += 1;
            if (exclusiveMaximum && max != null) max -= 1;
            if (min != null && min >= 0) {
                unsigned = true;
            }
            columnDefinition.put("colDataType", "DECIMAL");
            columnDefinition.put("colUnsigned", unsigned);
            columnDefinition.put("colDataTypeArguments", columnDataTypeArguments);
            columnDataTypeArguments.add(toCodegenPostgreSQLDataTypeArgument(20));
            columnDataTypeArguments.add(toCodegenPostgreSQLDataTypeArgument(9));
        }

        if (Boolean.TRUE.equals(required)) {
            columnDefinition.put("colNotNull", true);
        } else {
            columnDefinition.put("colNotNull", false);
            try {
                columnDefinition.put("colDefault", toCodegenPostgreSQLDataTypeDefault(defaultValue, (String) columnDefinition.get("colDataType")));
            } catch (RuntimeException exception) {
                LOGGER.warn(
                        "Property '{}' of model '{}' mapped to PostgreSQL data type which doesn't support default value",
                        baseName, model.getName());
                columnDefinition.put("colDefault", null);
            }
        }

        if (description != null) {
            columnDefinition.put("colComment", description);
        }
    }

    /**
     * Processes each model's property mapped to boolean type and adds related vendor extensions
     *
     * @param model    model
     * @param property model's property
     */
    public void processBooleanTypeProperty(CodegenModel model, CodegenProperty property) {
        Map<String, Object> vendorExtensions = property.getVendorExtensions();
        Map<String, Object> postgresqlSchema = new HashMap<>();
        Map<String, Object> columnDefinition = new HashMap<>();
        String modelName = model.getName();
        String tableName = this.toTableName(modelName);
        columnDefinition.put("tblName", tableName);
        ArrayList columnDataTypeArguments = new ArrayList();
        String baseName = property.getBaseName();
        String colName = this.toColumnName(baseName);
        String description = property.getDescription();
        String defaultValue = property.getDefaultValue();
        Boolean required = property.getRequired();

        if (vendorExtensions.containsKey(VENDOR_EXTENSION_POSTGRESQL_SCHEMA)) {
            // user already specified schema values
            LOGGER.info("Found vendor extension in '{}' property, autogeneration skipped", baseName);
            return;
        }

        if (this.getIdentifierNamingConvention().equals("snake_case") && !baseName.equals(colName)) {
            // add original name in column comment
            String commentExtra = "Original param name - " + baseName + ".";
            description = (description == null || description.isEmpty()) ? commentExtra : description + ". " + commentExtra;
        }

        vendorExtensions.put(VENDOR_EXTENSION_POSTGRESQL_SCHEMA, postgresqlSchema);
        postgresqlSchema.put("columnDefinition", columnDefinition);
        columnDefinition.put("colName", colName);
        columnDefinition.put("colDataType", "BOOLEAN");
        columnDefinition.put("colDataTypeArguments", columnDataTypeArguments);

        if (Boolean.TRUE.equals(required)) {
            columnDefinition.put("colNotNull", true);
        } else {
            columnDefinition.put("colNotNull", false);
            try {
                columnDefinition.put("colDefault", toCodegenPostgreSQLDataTypeDefault(defaultValue, (String) columnDefinition.get("colDataType")));
            } catch (RuntimeException exception) {
                LOGGER.warn(
                        "Property '{}' of model '{}' mapped to PostgreSQL data type which doesn't support default value",
                        baseName, model.getName());
                columnDefinition.put("colDefault", null);
            }
        }

        if (description != null) {
            columnDefinition.put("colComment", description);
        }
    }

    /**
     * Processes each model's property mapped to string type and adds related vendor extensions
     *
     * @param model    model
     * @param property model's property
     */
    public void processStringTypeProperty(CodegenModel model, CodegenProperty property) {
        Map<String, Object> vendorExtensions = property.getVendorExtensions();
        Map<String, Object> postgresqlSchema = new HashMap<>();
        Map<String, Object> columnDefinition = new HashMap<>();
        String modelName = model.getName();
        String tableName = this.toTableName(modelName);
        columnDefinition.put("tblName", tableName);
        ArrayList columnDataTypeArguments = new ArrayList();
        String baseName = property.getBaseName();
        String colName = this.toColumnName(baseName);
        String dataType = property.getDataType();
        String dataFormat = property.getDataFormat();
        String description = property.getDescription();
        Integer minLength = property.getMinLength();
        Integer maxLength = property.getMaxLength();
        String defaultValue = property.getDefaultValue();
        Boolean required = property.getRequired();
        Boolean isEnum = property.isEnum;

        if (vendorExtensions.containsKey(VENDOR_EXTENSION_POSTGRESQL_SCHEMA)) {
            // user already specified schema values
            LOGGER.info("Found vendor extension in '{}' property, autogeneration skipped", baseName);
            return;
        }

        if (this.getIdentifierNamingConvention().equals("snake_case") && !baseName.equals(colName)) {
            // add original name in column comment
            String commentExtra = "Original param name - " + baseName + ".";
            description = (description == null || description.isEmpty()) ? commentExtra : description + ". " + commentExtra;
        }

        vendorExtensions.put(VENDOR_EXTENSION_POSTGRESQL_SCHEMA, postgresqlSchema);
        postgresqlSchema.put("columnDefinition", columnDefinition);
        columnDefinition.put("colName", colName);

        if (Boolean.TRUE.equals(isEnum)) {
            Map<String, Object> allowableValues = property.getAllowableValues();
            List<Object> enumValues = (List<Object>) allowableValues.get("values");
            columnDefinition.put("colDataType", "ENUM");
            columnDefinition.put("colDataTypeArguments", columnDataTypeArguments);
            for (int i = 0; i < enumValues.size(); i++) {
                if (i > ENUM_MAX_ELEMENTS - 1) {
                    LOGGER.warn(
                            "ENUM column can have maximum of {} distinct elements, following value will be skipped: {}",
                            ENUM_MAX_ELEMENTS, (String) enumValues.get(i));
                    break;
                }
                String value = String.valueOf(enumValues.get(i));
                columnDataTypeArguments.add(toCodegenPostgreSQLDataTypeArgument(value));
            }
        } else if (dataType.equals("BYTEA")) {
            columnDefinition.put("colDataType", "BYTEA");
        } else {
            String matchedStringType = getPostgreSQLMatchedStringDataType(minLength, maxLength);
            columnDefinition.put("colDataType", matchedStringType);
            if (matchedStringType.equals("CHAR") || matchedStringType.equals("VARCHAR")) {
                columnDefinition.put("colDataTypeArguments", columnDataTypeArguments);
                columnDataTypeArguments.add(toCodegenPostgreSQLDataTypeArgument((maxLength != null) ? maxLength : 255));
            }
        }

        if (Boolean.TRUE.equals(required)) {
            columnDefinition.put("colNotNull", true);
        } else {
            columnDefinition.put("colNotNull", false);
            try {
                columnDefinition.put("colDefault", toCodegenPostgreSQLDataTypeDefault(defaultValue, (String) columnDefinition.get("colDataType")));
            } catch (RuntimeException exception) {
                LOGGER.warn(
                        "Property '{}' of model '{}' mapped to PostgreSQL data type which doesn't support default value",
                        baseName, model.getName());
                columnDefinition.put("colDefault", null);
            }
        }

        if (description != null) {
            columnDefinition.put("colComment", description);
        }
    }

    /**
     * Processes each model's property mapped to date type and adds related vendor extensions
     *
     * @param model    model
     * @param property model's property
     */
    public void processDateTypeProperty(CodegenModel model, CodegenProperty property) {
        Map<String, Object> vendorExtensions = property.getVendorExtensions();
        Map<String, Object> postgresqlSchema = new HashMap<>();
        Map<String, Object> columnDefinition = new HashMap<>();
        String modelName = model.getName();
        String tableName = this.toTableName(modelName);
        columnDefinition.put("tblName", tableName);
        String baseName = property.getBaseName();
        String colName = this.toColumnName(baseName);
        String dataType = property.getDataType();
        Boolean required = property.getRequired();
        String description = property.getDescription();
        String defaultValue = property.getDefaultValue();

        if (vendorExtensions.containsKey(VENDOR_EXTENSION_POSTGRESQL_SCHEMA)) {
            // user already specified schema values
            LOGGER.info("Found vendor extension in '{}' property, autogeneration skipped", baseName);
            return;
        }

        if (this.getIdentifierNamingConvention().equals("snake_case") && !baseName.equals(colName)) {
            // add original name in column comment
            String commentExtra = "Original param name - " + baseName + ".";
            description = (description == null || description.isEmpty()) ? commentExtra : description + ". " + commentExtra;
        }

        vendorExtensions.put(VENDOR_EXTENSION_POSTGRESQL_SCHEMA, postgresqlSchema);
        postgresqlSchema.put("columnDefinition", columnDefinition);
        columnDefinition.put("colName", colName);
        columnDefinition.put("colDataType", dataType);

        if (Boolean.TRUE.equals(required)) {
            columnDefinition.put("colNotNull", true);
        } else {
            columnDefinition.put("colNotNull", false);
            try {
                columnDefinition.put("colDefault", toCodegenPostgreSQLDataTypeDefault(defaultValue, (String) columnDefinition.get("colDataType")));
            } catch (RuntimeException exception) {
                LOGGER.warn(
                        "Property '{}' of model '{}' mapped to PostgreSQL data type which doesn't support default value",
                        baseName, model.getName());
                columnDefinition.put("colDefault", null);
            }
        }

        if (description != null) {
            columnDefinition.put("colComment", description);
        }
    }

    /**
     * Processes each model's property mapped to JSON type and adds related vendor extensions
     *
     * @param model    model
     * @param property model's property
     */
    public void processJsonTypeProperty(CodegenModel model, CodegenProperty property) {
        Map<String, Object> vendorExtensions = property.getVendorExtensions();
        Map<String, Object> postgresqlSchema = new HashMap<>();
        Map<String, Object> columnDefinition = new HashMap<>();
        String modelName = model.getName();
        String tableName = this.toTableName(modelName);
        columnDefinition.put("tblName", tableName);
        String baseName = property.getBaseName();
        String colName = this.toColumnName(baseName);
        String dataType = property.getDataType();
        Boolean required = property.getRequired();
        String description = property.getDescription();
        String defaultValue = property.getDefaultValue();

        if (vendorExtensions.containsKey(VENDOR_EXTENSION_POSTGRESQL_SCHEMA)) {
            // user already specified schema values
            LOGGER.info("Found vendor extension in '{}' property, autogeneration skipped", baseName);
            return;
        }

        if (this.getIdentifierNamingConvention().equals("snake_case") && !baseName.equals(colName)) {
            // add original name in column comment
            String commentExtra = "Original param name - " + baseName + ".";
            description = (description == null || description.isEmpty()) ? commentExtra : description + ". " + commentExtra;
        }

        vendorExtensions.put(VENDOR_EXTENSION_POSTGRESQL_SCHEMA, postgresqlSchema);
        postgresqlSchema.put("columnDefinition", columnDefinition);
        columnDefinition.put("colName", colName);
        columnDefinition.put("colDataType", dataType);
        if (Boolean.FALSE.equals(getJsonDataTypeEnabled())) {
            columnDefinition.put("colDataType", "TEXT");
        }

        if (Boolean.TRUE.equals(required)) {
            columnDefinition.put("colNotNull", true);
        } else {
            columnDefinition.put("colNotNull", false);
            try {
                columnDefinition.put("colDefault", toCodegenPostgreSQLDataTypeDefault(defaultValue, (String) columnDefinition.get("colDataType")));
            } catch (RuntimeException exception) {
                LOGGER.warn(
                        "Property '{}' of model '{}' mapped to PostgreSQL data type which doesn't support default value",
                        baseName, model.getName());
                columnDefinition.put("colDefault", null);
            }
        }

        if (description != null) {
            columnDefinition.put("colComment", description);
        }
    }

    /**
     * Processes each model's property not mapped to any type and adds related vendor extensions
     * Most of time it's related to referenced properties eg. \Model\User
     *
     * @param model    model
     * @param property model's property
     */
    public void processUnknownTypeProperty(CodegenModel model, CodegenProperty property) {
        Map<String, Object> vendorExtensions = property.getVendorExtensions();
        Map<String, Object> postgresqlSchema = new HashMap<>();
        Map<String, Object> columnDefinition = new HashMap<>();
        String modelName = model.getName();
        String tableName = this.toTableName(modelName);
        columnDefinition.put("tblName", tableName);
        String baseName = property.getBaseName();
        String colName = this.toColumnName(baseName);
        Boolean required = property.getRequired();
        String description = property.getDescription();
        String defaultValue = property.getDefaultValue();

        if (vendorExtensions.containsKey(VENDOR_EXTENSION_POSTGRESQL_SCHEMA)) {
            // user already specified schema values
            LOGGER.info("Found vendor extension in '{}' property, autogeneration skipped", baseName);
            return;
        }

        if (this.getIdentifierNamingConvention().equals("snake_case") && !baseName.equals(colName)) {
            // add original name in column comment
            String commentExtra = "Original param name - " + baseName + ".";
            description = (description == null || description.isEmpty()) ? commentExtra : description + ". " + commentExtra;
        }

        vendorExtensions.put(VENDOR_EXTENSION_POSTGRESQL_SCHEMA, postgresqlSchema);
        postgresqlSchema.put("columnDefinition", columnDefinition);
        columnDefinition.put("colName", colName);
        columnDefinition.put("colDataType", "TEXT");

        if (Boolean.TRUE.equals(required)) {
            columnDefinition.put("colNotNull", true);
        } else {
            columnDefinition.put("colNotNull", false);
            try {
                columnDefinition.put("colDefault", toCodegenPostgreSQLDataTypeDefault(defaultValue, (String) columnDefinition.get("colDataType")));
            } catch (RuntimeException exception) {
                LOGGER.warn(
                        "Property '{}' of model '{}' mapped to PostgreSQL data type which doesn't support default value",
                        baseName, model.getName());
                columnDefinition.put("colDefault", null);
            }
        }

        if (description != null) {
            columnDefinition.put("colComment", description);
        }
    }

    /**
     * Generates codegen property for PostgreSQL data type argument
     *
     * @param value   argument value
     * @return generated codegen property
     */
    public HashMap<String, Object> toCodegenPostgreSQLDataTypeArgument(Object value) {
        HashMap<String, Object> arg = new HashMap<>();
        if (value instanceof String) {
            arg.put("isString", true);
            arg.put("isFloat", false);
            arg.put("isInteger", false);
            arg.put("isNumeric", false);
        } else if (value instanceof Integer || value instanceof Long) {
            arg.put("isString", false);
            arg.put("isFloat", false);
            arg.put("isInteger", true);
            arg.put("isNumeric", true);
        } else if (value instanceof Number) {
            arg.put("isString", false);
            arg.put("isFloat", true);
            arg.put("isInteger", false);
            arg.put("isNumeric", true);
        } else {
            LOGGER.warn("PostgreSQL data type argument can be primitive type only. Class '{}' is provided", value.getClass());
        }
        arg.put("argumentValue", value);
        return arg;
    }

    /**
     * Generates default codegen property for PostgreSQL column definition
     * Ref: https://dev.postgresql.com/doc/refman/5.7/en/data-type-defaults.html
     *
     * @param defaultValue  value
     * @param postgresqlDataType PostgreSQL data type
     * @return generated codegen property
     */
    public HashMap<String, Object> toCodegenPostgreSQLDataTypeDefault(String defaultValue, String postgresqlDataType) {
        HashMap<String, Object> defaultMap = new HashMap<>();
        if (defaultValue == null || defaultValue.toUpperCase(Locale.ROOT).equals("NULL")) {
            defaultMap.put("defaultValue", "NULL");
            defaultMap.put("isString", false);
            defaultMap.put("isNumeric", false);
            defaultMap.put("isKeyword", true);
            return defaultMap;
        }

        switch (postgresqlDataType.toUpperCase(Locale.ROOT)) {
            case "SMALLINT":
            case "INTEGER":
            case "BIGINT":
                // SERIAL DEFAULT VALUE is a special case. In the definition of an integer column, it is an alias for NOT NULL AUTO_INCREMENT UNIQUE
                if (defaultValue.equals("SERIAL DEFAULT VALUE")) {
                    defaultMap.put("defaultValue", defaultValue);
                    defaultMap.put("isString", false);
                    defaultMap.put("isNumeric", false);
                    defaultMap.put("isKeyword", true);
                } else {
                    defaultMap.put("defaultValue", defaultValue);
                    defaultMap.put("isString", false);
                    defaultMap.put("isNumeric", true);
                    defaultMap.put("isKeyword", false);
                }
                return defaultMap;
            case "TIMESTAMP":
            case "DATE":
                // The exception is that, for TIMESTAMP and DATE columns, you can specify CURRENT_TIMESTAMP as the default
                if (defaultValue.equals("CURRENT_TIMESTAMP")) {
                    defaultMap.put("defaultValue", defaultValue);
                    defaultMap.put("isString", false);
                    defaultMap.put("isNumeric", false);
                    defaultMap.put("isKeyword", true);
                } else {
                    defaultMap.put("defaultValue", defaultValue);
                    defaultMap.put("isString", true);
                    defaultMap.put("isNumeric", false);
                    defaultMap.put("isKeyword", false);
                }
                return defaultMap;
            case "TINYBLOB":
            case "BLOB":
            case "BYTEA":
            case "LONGBLOB":
            case "TINYTEXT":
            case "TEXT":
            case "MEDIUMTEXT":
            case "LONGTEXT":
            case "GEOMETRY":
            case "JSON":
                // The BLOB, TEXT, GEOMETRY, and JSON data types cannot be assigned a default value.
                throw new RuntimeException("The BLOB, TEXT, GEOMETRY, and JSON data types cannot be assigned a default value");
            default:
                defaultMap.put("defaultValue", defaultValue);
                defaultMap.put("isString", true);
                defaultMap.put("isNumeric", false);
                defaultMap.put("isKeyword", false);
                return defaultMap;
        }
    }

    /**
     * Finds best fitted PostgreSQL data type for integer variable based on minimum and maximum properties
     *
     * @param minimum  (optional) codegen property
     * @param maximum  (optional) codegen property
     * @param unsigned (optional) whether variable is unsigned or not
     * @return PostgreSQL integer data type
     */
    public String getPostgreSQLMatchedIntegerDataType(Long minimum, Long maximum, Boolean unsigned) {
        // we can choose fit postgresql data type
        // ref: http://www.postgres.cn/docs/12/datatype-numeric.html
        long min = (minimum != null) ? minimum : -2147483648L;
        long max = (maximum != null) ? maximum : 2147483647L;
        long actualMin = Math.min(min, max); // sometimes min and max values can be mixed up
        long actualMax = Math.max(min, max); // sometimes only minimum specified and it can be pretty high
        if (minimum != null && maximum != null && minimum > maximum) {
            LOGGER.warn("Codegen property 'minimum' cannot be greater than 'maximum'");
        }
        if (Boolean.TRUE.equals(unsigned) && actualMin >= 0) {
            if (actualMax <= 32767) {
                return "SMALLSERIAL";
            } else if (actualMax <= 2147483647) {
                return "SERIAL";
            }  else if (actualMax > 9223372036854775807L) {
                return "BIGSERIAL";
            }
        } else {
            if (actualMin >= -32768 && actualMax <= 32767) {
                return "SMALLINT";
            } else if (actualMin >= -2147483648  && actualMax <= 2147483647) {
                return "INTEGER";
            } else if (actualMin >= -9223372036854775808L  && actualMax <= 9223372036854775807L ) {
                return "BIGINT";
            }
        }
        return "INTEGER";
    }

    /**
     * Finds best fitted PostgreSQL data type for string variable based on minLength and maxLength properties
     *
     * @param minLength (optional) codegen property
     * @param maxLength (optional) codegen property
     * @return PostgreSQL string data type
     */
    public String getPostgreSQLMatchedStringDataType(Integer minLength, Integer maxLength) {
        // we can choose fit postgresql data type
        // ref: https://dev.postgresql.com/doc/refman/8.0/en/string-type-overview.html
        int min = (minLength != null && minLength >= 0) ? minLength : 0;
        // 未指定最大长度时，默认255 的字符串
        int max = (maxLength != null && maxLength >= 0) ? maxLength : 255;
        Integer actualMin = Math.min(min, max); // sometimes minLength and maxLength values can be mixed up
        Integer actualMax = Math.max(min, max); // sometimes only minLength specified and it can be pretty high
        if (minLength != null && maxLength != null && minLength > maxLength) {
            LOGGER.warn("Codegen property 'minLength' cannot be greater than 'maxLength'");
        }
        if (actualMax.equals(actualMin) && actualMax <= 255) {
            return "CHAR";
        } else if (actualMax <= 255) {
            return "VARCHAR";
        } else if (actualMax > 255 && actualMax <= Integer.MAX_VALUE) {
            return "TEXT";
        }
        return "TEXT";
    }

    /**
     * Checks whether string is one of PostgreSQL Data Types
     * Ref: https://dev.postgresql.com/doc/refman/8.0/en/data-type-overview.html
     *
     * @param dataType which needs to check
     * @return true if value is correct PostgreSQL data type, otherwise false
     */
    public Boolean isPostgreSQLDataType(String dataType) {
        return (
                postgresqlNumericTypes.contains(dataType.toUpperCase(Locale.ROOT)) ||
                        postgresqlDateAndTimeTypes.contains(dataType.toUpperCase(Locale.ROOT)) ||
                        postgresqlStringTypes.contains(dataType.toUpperCase(Locale.ROOT)) ||
                        postgresqlSpatialTypes.contains(dataType.toUpperCase(Locale.ROOT)) ||
                        dataType.toUpperCase(Locale.ROOT).equals("JSON")
        );
    }

    /**
     * Converts name to valid PostgreSQL database name
     * Produced name must be used with backticks only, eg. `database_name`
     *
     * @param name source name
     * @return database name
     */
    public String toDatabaseName(String name) {
        String identifier = toPostgreSQLIdentifier(name, databaseNamePrefix, databaseNameSuffix);
        if (identifier.length() > IDENTIFIER_MAX_LENGTH) {
            LOGGER.warn("Database name cannot exceed 64 chars. Name '{}' will be truncated", name);
            identifier = identifier.substring(0, IDENTIFIER_MAX_LENGTH);
        }
        return identifier;
    }

    /**
     * Converts name to valid PostgreSQL column name
     * Produced name must be used with backticks only, eg. `table_name`
     *
     * @param name source name
     * @return table name
     */
    public String toTableName(String name) {
        String identifier = toPostgreSQLIdentifier(name, tableNamePrefix, tableNameSuffix);
        if (identifierNamingConvention.equals("snake_case")) {
            identifier = underscore(identifier);
        }
        if (identifier.length() > IDENTIFIER_MAX_LENGTH) {
            LOGGER.warn("Table name cannot exceed 64 chars. Name '{}' will be truncated", name);
            identifier = identifier.substring(0, IDENTIFIER_MAX_LENGTH);
        }
        return identifier;
    }

    /**
     * Converts name to valid PostgreSQL column name
     * Produced name must be used with backticks only, eg. `column_name`
     *
     * @param name source name
     * @return column name
     */
    public String toColumnName(String name) {
        String identifier = toPostgreSQLIdentifier(name, columnNamePrefix, columnNameSuffix);
        if (identifierNamingConvention.equals("snake_case")) {
            identifier = underscore(identifier);
        }
        if (identifier.length() > IDENTIFIER_MAX_LENGTH) {
            LOGGER.warn("Column name cannot exceed 64 chars. Name '{}' will be truncated", name);
            identifier = identifier.substring(0, IDENTIFIER_MAX_LENGTH);
        }
        return identifier;
    }

    /**
     * Converts name to valid PostgreSQL identifier which can be used as database, table, column name
     * Produced name must be used with backticks only, eg. `column_name`
     *
     * @param name   source name
     * @param prefix when escaped name is digits only, prefix will be prepended
     * @param suffix when escaped name is digits only, suffix will be appended
     * @return identifier name
     */
    public String toPostgreSQLIdentifier(String name, String prefix, String suffix) {
        String escapedName = escapePostgreSQLQuotedIdentifier(name);
        // Database, table, and column names cannot end with space characters.
        if (escapedName.matches(".*\\s$")) {
            LOGGER.warn("Database, table, and column names cannot end with space characters. Check '{}' name", name);
            escapedName = escapedName.replaceAll("\\s+$", "");
        }

        // Identifiers may begin with a digit but unless quoted may not consist solely of digits.
        if (escapedName.matches("^\\d+$")) {
            LOGGER.warn("Database, table, and column names cannot consist solely of digits. Check '{}' name", name);
            escapedName = prefix + escapedName + suffix;
        }else{
            escapedName = prefix + escapedName;
        }

        // identifier name cannot be empty
        if (escapedName.isEmpty()) {
            throw new RuntimeException("Empty database/table/column name for property '" + name + "' not allowed");
        }
        return escapedName;
    }

    /**
     * Escapes PostgreSQL identifier to use it in SQL statements without backticks, eg. SELECT identifier FROM
     * Ref: https://dev.postgresql.com/doc/refman/8.0/en/identifiers.html
     *
     * @param identifier source identifier
     * @return escaped identifier
     */
    public String escapePostgreSQLUnquotedIdentifier(String identifier) {
        // ASCII: [0-9,a-z,A-Z$_] (basic Latin letters, digits 0-9, dollar, underscore) Extended: U+0080 .. U+FFFF
        Pattern regexp = Pattern.compile("[^0-9a-zA-z$_\\u0080-\\uFFFF]");
        Matcher matcher = regexp.matcher(identifier);
        if (matcher.find()) {
            LOGGER.warn("Identifier '{}' contains unsafe characters out of [0-9,a-z,A-Z$_] and U+0080..U+FFFF range",
                    identifier);
            identifier = identifier.replaceAll("[^0-9a-zA-z$_\\u0080-\\uFFFF]", "");
        }

        // ASCII NUL (U+0000) and supplementary characters (U+10000 and higher) are not permitted in quoted or unquoted identifiers.
        // Don't know how to match these characters, hope that first regexp already strip them
        // Pattern regexp2 = Pattern.compile("[\0\uD800\uDC00-\uDBFF\uDFFF]");
        return identifier;
    }

    /**
     * Escapes PostgreSQL identifier to use it in SQL statements with backticks, eg. SELECT `identifier` FROM
     * Ref: https://dev.postgresql.com/doc/refman/8.0/en/identifiers.html
     *
     * @param identifier source identifier
     * @return escaped identifier
     */
    public String escapePostgreSQLQuotedIdentifier(String identifier) {
        // ASCII: U+0001 .. U+007F Extended: U+0080 .. U+FFFF
        Pattern regexp = Pattern.compile("[^\\u0001-\\u007F\\u0080-\\uFFFF]");
        Matcher matcher = regexp.matcher(identifier);
        if (matcher.find()) {
            LOGGER.warn("Identifier '{}' contains unsafe characters out of U+0001..U+007F and U+0080..U+FFFF range",
                    identifier);
            identifier = identifier.replaceAll("[^\\u0001-\\u007F\\u0080-\\uFFFF]", "");
        }

        // ASCII NUL (U+0000) and supplementary characters (U+10000 and higher) are not permitted in quoted or unquoted identifiers.
        // Don't know how to match these characters, hope that first regexp already strip them
        // Pattern regexp2 = Pattern.compile("[\0\uD800\uDC00-\uDBFF\uDFFF]");
        return identifier;
    }

    @Override
    public String escapeReservedWord(String name) {
        LOGGER.warn("'{}' is PostgreSQL reserved word. Do not use that word or properly escape it with backticks in mustache template", name);
        return name;
    }

    @Override
    public String escapeQuotationMark(String input) {
        // remove ' to avoid code injection
        return input.replace("'", "");
    }

    @Override
    public String escapeUnsafeCharacters(String input) {
        return input.replace("*/", "*_/").replace("/*", "/_*");
    }

    /**
     * Sets default database name for all PostgreSQL queries
     * Provided value will be escaped when necessary
     *
     * @param databaseName source name
     */
    public void setDefaultDatabaseName(String databaseName) {
        String escapedName = toDatabaseName(databaseName);
        if (!escapedName.equals(databaseName)) {
            LOGGER.error(
                    "Invalid database name. '{}' cannot be used as PostgreSQL identifier. Escaped value '{}' will be used instead.",
                    databaseName, escapedName);
        }
        this.defaultDatabaseName = escapedName;
    }

    /**
     * Sets identifier naming convention for table names and column names.
     * This is not related to database name which is defined by defaultDatabaseName option.
     *
     * @param naming identifier naming convention (original|snake_case)
     */
    public void setIdentifierNamingConvention(String naming) {
        switch (naming) {
            case "original":
            case "snake_case":
                this.identifierNamingConvention = naming;
                break;
            default:
                LOGGER.warn("\"{}\" is invalid \"identifierNamingConvention\" argument. Current \"{}\" used instead.",
                        naming, this.identifierNamingConvention);
        }
    }

    /**
     * Slightly modified version of AbstractPhpCodegen.toSrcPath method.
     *
     * @param packageName package name
     *
     * @return path
     */
    public String toSrcPath(String packageName) {
        // Trim prefix file separators from package path
        String packagePath = StringUtils.removeStart(
            // Replace period, backslash, forward slash with file separator in package name
            packageName.replaceAll("[\\.\\\\/]", Matcher.quoteReplacement("/")),
            File.separator
        );

        // Trim trailing file separators from the overall path
        return StringUtils.removeEnd(packagePath, File.separator);
    }

    @Override
    public GeneratorLanguage generatorLanguage() { return GeneratorLanguage.POSTGRESQL; }
}
