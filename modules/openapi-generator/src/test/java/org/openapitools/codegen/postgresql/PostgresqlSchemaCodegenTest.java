package org.openapitools.codegen.postgresql;

import org.openapitools.codegen.languages.PostgresqlSchemaCodegen;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;

public class PostgresqlSchemaCodegenTest {

    @Test
    public void testGetPostgresqlMatchedIntegerDataType() {
        final PostgresqlSchemaCodegen codegen = new PostgresqlSchemaCodegen();
        Assert.assertSame(codegen.getPostgreSQLMatchedIntegerDataType(null, null, null), "INTEGER");

        Assert.assertSame(codegen.getPostgreSQLMatchedIntegerDataType(-32768L, 32767l, false), "SMALLINT");
        Assert.assertSame(codegen.getPostgreSQLMatchedIntegerDataType(0L, 255L, true), "SMALLSERIAL");

        Assert.assertSame(codegen.getPostgreSQLMatchedIntegerDataType(500L, 100L, null), "SMALLINT");
        Assert.assertSame(codegen.getPostgreSQLMatchedIntegerDataType(500L, 100L, true), "SMALLSERIAL");
        Assert.assertSame(codegen.getPostgreSQLMatchedIntegerDataType(500L, 100L, false), "SMALLINT");
        Assert.assertSame(codegen.getPostgreSQLMatchedIntegerDataType(-32768L, 32767L, false), "SMALLINT");
        Assert.assertSame(codegen.getPostgreSQLMatchedIntegerDataType(0L, 32767L, true), "SMALLSERIAL");

        Assert.assertSame(codegen.getPostgreSQLMatchedIntegerDataType(-8388608L, 8388607L, false), "INTEGER");
        Assert.assertSame(codegen.getPostgreSQLMatchedIntegerDataType(0L, 16777215L, true), "SERIAL");

        Assert.assertSame(codegen.getPostgreSQLMatchedIntegerDataType(-2147483648L, 2147483647L, false), "INTEGER");
        Assert.assertSame(codegen.getPostgreSQLMatchedIntegerDataType(Long.parseLong(String.valueOf(Integer.MIN_VALUE)), Long.parseLong(String.valueOf(Integer.MAX_VALUE)), false), "INTEGER");
        Assert.assertSame(codegen.getPostgreSQLMatchedIntegerDataType(0L, 4294967295L, true), "INTEGER");

        Assert.assertSame(codegen.getPostgreSQLMatchedIntegerDataType(-2147483649L, 2147483648L, false), "BIGINT");
        Assert.assertSame(codegen.getPostgreSQLMatchedIntegerDataType(0L, 4294967296L, true), "INTEGER");
    }

    @Test
    public void testGetPostgresqlMatchedStringDataType() {
        final PostgresqlSchemaCodegen codegen = new PostgresqlSchemaCodegen();
        Assert.assertSame(codegen.getPostgreSQLMatchedStringDataType(6, 6), "CHAR");
        Assert.assertSame(codegen.getPostgreSQLMatchedStringDataType(0, 0), "CHAR");
        Assert.assertSame(codegen.getPostgreSQLMatchedStringDataType(255, 255), "CHAR");

        Assert.assertSame(codegen.getPostgreSQLMatchedStringDataType(null, 100), "VARCHAR");
        Assert.assertSame(codegen.getPostgreSQLMatchedStringDataType(null, 255), "VARCHAR");
        Assert.assertSame(codegen.getPostgreSQLMatchedStringDataType(50, 255), "VARCHAR");
        Assert.assertSame(codegen.getPostgreSQLMatchedStringDataType(100, 20), "VARCHAR");

        Assert.assertSame(codegen.getPostgreSQLMatchedStringDataType(null, null), "TEXT");
        Assert.assertSame(codegen.getPostgreSQLMatchedStringDataType(100, null), "TEXT");
        Assert.assertSame(codegen.getPostgreSQLMatchedStringDataType(255, null), "TEXT");
        Assert.assertSame(codegen.getPostgreSQLMatchedStringDataType(null, 256), "TEXT");

    }

    @Test
    public void testToCodegenPostgresqlDataTypeArgument() {
        final PostgresqlSchemaCodegen codegen = new PostgresqlSchemaCodegen();
        String strArgument = "HelloWorld";
        HashMap<String, Object> strProp = codegen.toCodegenPostgreSQLDataTypeArgument(strArgument);
        Assert.assertTrue((Boolean) strProp.get("isString"));
        Assert.assertFalse((Boolean) strProp.get("isFloat"));
        Assert.assertFalse((Boolean) strProp.get("isInteger"));
        Assert.assertFalse((Boolean) strProp.get("isNumeric"));
        Assert.assertSame(strProp.get("argumentValue"), strArgument);

        Integer intArgument = 10;
        HashMap<String, Object> intProp = codegen.toCodegenPostgreSQLDataTypeArgument(intArgument);
        Assert.assertFalse((Boolean) intProp.get("isString"));
        Assert.assertFalse((Boolean) intProp.get("isFloat"));
        Assert.assertTrue((Boolean) intProp.get("isInteger"));
        Assert.assertTrue((Boolean) intProp.get("isNumeric"));
        Assert.assertSame(intProp.get("argumentValue"), intArgument);

        Double floatArgument = 3.14;
        HashMap<String, Object> floatProp = codegen.toCodegenPostgreSQLDataTypeArgument(floatArgument);
        Assert.assertFalse((Boolean) floatProp.get("isString"));
        Assert.assertTrue((Boolean) floatProp.get("isFloat"));
        Assert.assertFalse((Boolean) floatProp.get("isInteger"));
        Assert.assertTrue((Boolean) floatProp.get("isNumeric"));
        Assert.assertSame(floatProp.get("argumentValue"), floatArgument);
    }

    @Test
    public void testToCodegenPostgresqlDataTypeDefault() {
        final PostgresqlSchemaCodegen codegen = new PostgresqlSchemaCodegen();
        HashMap<String, Object> defaultMap = null;
        ArrayList<String> intFixture = new ArrayList<String>(Arrays.asList(
            "SMALLINT", "SmallInt", "SmallInt", "BIGINT"
        ));
        for(String intType : intFixture) {
            defaultMap = codegen.toCodegenPostgreSQLDataTypeDefault("150", intType);
            Assert.assertTrue((Boolean) defaultMap.get("isNumeric"));
            Assert.assertFalse((Boolean) defaultMap.get("isString"));
            Assert.assertFalse((Boolean) defaultMap.get("isKeyword"));
            Assert.assertSame(defaultMap.get("defaultValue"), "150");
        }
        defaultMap = codegen.toCodegenPostgreSQLDataTypeDefault("SERIAL DEFAULT VALUE", "TINYINT");
        Assert.assertFalse((Boolean) defaultMap.get("isNumeric"));
        Assert.assertFalse((Boolean) defaultMap.get("isString"));
        Assert.assertTrue((Boolean) defaultMap.get("isKeyword"));
        Assert.assertSame(defaultMap.get("defaultValue"), "SERIAL DEFAULT VALUE");

        ArrayList<String> dateFixture = new ArrayList<String>(Arrays.asList(
            "Timestamp", "DateTime"
        ));
        for(String dateType : dateFixture) {
            defaultMap = codegen.toCodegenPostgreSQLDataTypeDefault("2018-08-12", dateType);
            Assert.assertFalse((Boolean) defaultMap.get("isNumeric"));
            Assert.assertTrue((Boolean) defaultMap.get("isString"));
            Assert.assertFalse((Boolean) defaultMap.get("isKeyword"));
            Assert.assertSame(defaultMap.get("defaultValue"), "2018-08-12");
        }
        defaultMap = codegen.toCodegenPostgreSQLDataTypeDefault("CURRENT_TIMESTAMP", "Timestamp");
        Assert.assertFalse((Boolean) defaultMap.get("isNumeric"));
        Assert.assertFalse((Boolean) defaultMap.get("isString"));
        Assert.assertTrue((Boolean) defaultMap.get("isKeyword"));
        Assert.assertSame(defaultMap.get("defaultValue"), "CURRENT_TIMESTAMP");

        ArrayList<String> restFixture = new ArrayList<String>(Arrays.asList(
            "VARCHAR", "CHAR", "ENUM", "UNKNOWN"
        ));
        for(String restType : restFixture) {
            defaultMap = codegen.toCodegenPostgreSQLDataTypeDefault("sometext", restType);
            Assert.assertFalse((Boolean) defaultMap.get("isNumeric"));
            Assert.assertTrue((Boolean) defaultMap.get("isString"));
            Assert.assertFalse((Boolean) defaultMap.get("isKeyword"));
            Assert.assertSame(defaultMap.get("defaultValue"), "sometext");
        }
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testToCodegenPostgresqlDataTypeDefaultWithExceptionalColumnType() {
        final PostgresqlSchemaCodegen codegen = new PostgresqlSchemaCodegen();
        HashMap<String, Object> defaultMap = null;
        ArrayList<String> specialFixture = new ArrayList<String>(Arrays.asList(
            "TINYBLOB", "Blob", "MEDIUMBLOB", "LONGBLOB", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT", "GEOMETRY", "JSON"
        ));
        for(String specialType : specialFixture) {
            defaultMap = codegen.toCodegenPostgreSQLDataTypeDefault("2018-08-12", specialType);
            Assert.assertNull(defaultMap);
        }
    }

    @Test
    public void testIsPostgresqlDataType() {
        final PostgresqlSchemaCodegen codegen = new PostgresqlSchemaCodegen();
        ArrayList<String> trueFixture = new ArrayList<String>(Arrays.asList(
            "INTEGER", "integer", "Integer", "DATE", "date", "Date", "VARCHAR", "varchar", "VarChar", "POINT", "Point", "point", "JSON", "json", "Json"
        ));
        ArrayList<String> falseFixture = new ArrayList<String>(Arrays.asList(
            "unknown", "HashMap", "HASHMAP", "hashmap"
        ));
        for(String trueValue : trueFixture) {
            Assert.assertTrue(codegen.isPostgreSQLDataType(trueValue), "'" + trueValue + "' isn't PostgreSQL data type");
        }
        for(String falseValue : falseFixture) {
            Assert.assertFalse(codegen.isPostgreSQLDataType(falseValue), "'" + falseValue + "' is PostgreSQL data type");
        }
    }

    @Test
    public void testToPostgreSQLIdentifier() {
        final PostgresqlSchemaCodegen codegen = new PostgresqlSchemaCodegen();
        Assert.assertEquals(codegen.toPostgreSQLIdentifier("table_name", "tbl_", ""), "table_name");
        Assert.assertEquals(codegen.toPostgreSQLIdentifier("table_name   ", "tbl_", ""), "table_name");
        Assert.assertEquals(codegen.toPostgreSQLIdentifier("12345678", "tbl_", ""), "tbl_12345678");
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testToPostgresqlIdentifierWithEmptyString() {
        final PostgresqlSchemaCodegen codegen = new PostgresqlSchemaCodegen();
        codegen.toPostgreSQLIdentifier("   ", "tbl_", "");
    }

    @Test
    public void testEscapePostgresqlUnquotedIdentifier() {
        final PostgresqlSchemaCodegen codegen = new PostgresqlSchemaCodegen();
        Assert.assertEquals(codegen.escapePostgreSQLUnquotedIdentifier("table1Z$_"), "table1Z$_");
        Assert.assertEquals(codegen.escapePostgreSQLUnquotedIdentifier("table1Z$_!#%~&?()*+-./"), "table1Z$_");
        Assert.assertEquals(codegen.escapePostgreSQLUnquotedIdentifier("table1Z$_русскийтекст"), "table1Z$_русскийтекст");
        Assert.assertEquals(codegen.escapePostgreSQLQuotedIdentifier("table𐀀"), "table");
        Assert.assertEquals(codegen.escapePostgreSQLQuotedIdentifier("table_name!'()�"), "table_name!'()�");
        Assert.assertEquals(codegen.escapePostgreSQLQuotedIdentifier("table_name𐌅𐌌 "), "table_name");
    }

    @Test
    public void testEscapePostgresqlQuotedIdentifier() {
        final PostgresqlSchemaCodegen codegen = new PostgresqlSchemaCodegen();
        Assert.assertEquals(codegen.escapePostgreSQLQuotedIdentifier("table"), "table");
        Assert.assertEquals(codegen.escapePostgreSQLQuotedIdentifier("table𐀀"), "table");
        Assert.assertEquals(codegen.escapePostgreSQLQuotedIdentifier("table_name!'()�"), "table_name!'()�");
        Assert.assertEquals(codegen.escapePostgreSQLQuotedIdentifier("table_name𐌅𐌌 "), "table_name");
    }

    @Test
    public void testIsReservedWord() {
        final PostgresqlSchemaCodegen codegen = new PostgresqlSchemaCodegen();
        Set<String> reservedWords = codegen.reservedWords();
        ArrayList<String> trueFixture = new ArrayList<String>(Arrays.asList(
                "abort", "absolute", "access", "action", "add", "admin", "after", "aggregate", "all", "also", "alter", "always", "analyse", "analyze", "and", "any",
                "array", "as", "asc", "assertion", "assignment", "asymmetric", "at", "attach", "attribute", "authorization", "backward", "before", "begin", "between",
                "bigint", "binary", "bit", "boolean", "both", "by", "cache", "call", "called", "cascade", "cascaded", "case", "cast", "catalog", "chain", "char", "character",
                "characteristics", "check", "checkpoint", "class", "close", "cluster", "coalesce", "collate", "collation", "column", "columns", "comment", "comments", "commit",
                "committed", "concurrently", "configuration", "conflict", "connection", "constraint", "constraints", "content", "continue", "conversion", "copy", "cost", "create",
                "cross", "csv", "cube", "current", "current_catalog", "current_date", "current_role", "current_schema", "current_time", "current_timestamp", "current_user", "cursor",
                "cycle", "data", "database", "day", "deallocate", "dec", "decimal", "declare", "default", "defaults", "deferrable", "deferred", "definer", "delete", "delimiter",
                "delimiters", "depends", "desc", "detach", "dictionary", "disable", "discard", "distinct", "do", "document", "domain", "double", "drop", "each", "else", "enable",
                "encoding", "encrypted", "end", "enum", "escape", "event", "except", "exclude", "excluding", "exclusive", "execute", "exists", "explain", "extension", "external",
                "extract", "false", "family", "fetch", "filter", "first", "float", "following", "for", "force", "foreign", "forward", "freeze", "from", "full", "function", "functions",
                "generated", "global", "grant", "granted", "greatest", "group", "grouping", "handler", "having", "header", "hold", "hour", "identity", "if", "ilike", "immediate",
                "immutable", "implicit", "import", "in", "including", "increment", "index", "indexes", "inherit", "inherits", "initially", "inline", "inner", "inout", "input",
                "insensitive", "insert", "instead", "int", "integer", "intersect", "interval", "into", "invoker", "is", "isnull", "isolation", "join", "key", "label", "language",
                "large", "last", "lateral", "leading", "leakproof", "least", "left", "level", "like", "limit", "listen", "load", "local", "localtime", "localtimestamp", "location",
                "lock", "locked", "logged", "mapping", "match", "materialized", "maxvalue", "method", "minute", "minvalue", "mode", "month", "move", "name", "names", "national",
                "natural", "nchar", "new", "next", "no", "none", "not", "nothing", "notify", "notnull", "nowait", "null", "nullif", "numeric", "object", "of", "off", "offset", "oids",
                "old", "on", "only", "operator", "option", "options", "or", "order", "ordinality", "out", "outer", "over", "overlaps", "overlay", "overriding", "owned", "owner", "parser",
                "partial", "partition", "passing", "password", "placing", "plans", "policy", "position", "preceding", "precision", "prepare", "prepared", "preserve", "primary", "prior",
                "privileges", "procedural", "procedure", "program", "publication", "quote", "range", "read", "real", "reassign", "recheck", "recursive", "ref", "references", "referencing",
                "refresh", "reindex", "relative", "release", "rename", "repeatable", "replace", "replica", "reset", "restart", "restrict", "returning", "returns", "revoke", "right", "role",
                "rollback", "rollup", "routine", "routines", "row", "rows", "rule", "savepoint", "schema", "schemas", "scroll", "search", "second", "security", "select", "sequence", "sequences",
                "serializable", "server", "session", "session_user", "set", "setof", "share", "show", "similar", "simple", "skip", "smallint", "snapshot", "some", "sql", "stable", "standalone",
                "start", "statement", "statistics", "stdin", "stdout", "storage", "strict", "strip", "subscription", "substring", "symmetric", "sysid", "system", "table", "tables", "tablesample",
                "tablespace", "temp", "template", "temporary", "text", "then", "ties", "time", "timestamp", "to", "trailing", "transaction", "transform", "treat", "trigger", "trim", "true", "truncate",
                "trusted", "type", "types", "unbounded", "uncommitted", "unencrypted", "union", "unique", "unknown", "unlisten", "unlogged", "until", "update", "user", "using", "vacuum", "valid",
                "validate", "validator", "value", "values", "varchar", "variadic", "varying", "verbose", "version", "view", "views", "volatile", "when", "where", "whitespace", "window", "with",
                "within", "without", "work", "wrapper", "write", "xml", "xmlattributes", "xmlconcat", "xmlelement", "xmlexists", "xmlforest", "xmlnamespaces", "xmlparse", "xmlpi", "xmlroot",
                "xmlserialize", "year", "yes", "zone"
        ));
        ArrayList<String> falseFixture = new ArrayList<String>(Arrays.asList(
             "bool", "charset", "cpu", "delay_key_write", "end_with", "format", "host", "install", "key_block_size",  "max_size", "quarter", "relay", "status", "datetime", "variables"
        ));
        for(String trueValue : trueFixture) {
            Assert.assertTrue(reservedWords.contains(trueValue), "'" + trueValue + "' isn't PostgreSQL reserved word");
        }
        for(String falseValue : falseFixture) {
            Assert.assertFalse(reservedWords.contains(falseValue), "'" + falseValue + "' is PostgreSQL reserved word");
        }
    }

    @Test
    public void testSetDefaultDatabaseName() {
        final PostgresqlSchemaCodegen codegen = new PostgresqlSchemaCodegen();
        codegen.setDefaultDatabaseName("valid_db_name");
        Assert.assertSame(codegen.getDefaultDatabaseName(), "valid_db_name");
        codegen.setDefaultDatabaseName("12345");
        Assert.assertNotSame(codegen.getDefaultDatabaseName(), "12345");
    }

    @Test
    public void testGetDefaultDatabaseName() {
        final PostgresqlSchemaCodegen codegen = new PostgresqlSchemaCodegen();
        Assert.assertSame(codegen.getDefaultDatabaseName(), "");
    }

    @Test
    public void testSetJsonDataTypeEnabled() {
        final PostgresqlSchemaCodegen codegen = new PostgresqlSchemaCodegen();
        codegen.setJsonDataTypeEnabled(true);
        Assert.assertTrue(codegen.getJsonDataTypeEnabled());
        codegen.setJsonDataTypeEnabled(false);
        Assert.assertFalse(codegen.getJsonDataTypeEnabled());
    }

    @Test
    public void testGetJsonDataTypeEnabled() {
        final PostgresqlSchemaCodegen codegen = new PostgresqlSchemaCodegen();
        Assert.assertTrue(codegen.getJsonDataTypeEnabled());
        codegen.setJsonDataTypeEnabled(false);
        Assert.assertFalse(codegen.getJsonDataTypeEnabled());
    }

    @Test
    public void testSetNamedParametersEnabled() {
        final PostgresqlSchemaCodegen codegen = new PostgresqlSchemaCodegen();
        codegen.setNamedParametersEnabled(true);
        Assert.assertTrue(codegen.getNamedParametersEnabled());
        codegen.setNamedParametersEnabled(false);
        Assert.assertFalse(codegen.getNamedParametersEnabled());
    }

    @Test
    public void testGetNamedParametersEnabled() {
        final PostgresqlSchemaCodegen codegen = new PostgresqlSchemaCodegen();
        Assert.assertFalse(codegen.getNamedParametersEnabled());
        codegen.setNamedParametersEnabled(true);
        Assert.assertTrue(codegen.getNamedParametersEnabled());
    }

    @Test
    public void testSetIdentifierNamingConvention() {
        final PostgresqlSchemaCodegen codegen = new PostgresqlSchemaCodegen();
        Assert.assertSame("original", codegen.getIdentifierNamingConvention());
        codegen.setIdentifierNamingConvention("invalidValue");
        Assert.assertSame("original", codegen.getIdentifierNamingConvention());
        codegen.setIdentifierNamingConvention("snake_case");
        Assert.assertSame("snake_case", codegen.getIdentifierNamingConvention());
        codegen.setIdentifierNamingConvention("anotherInvalid");
        Assert.assertSame("snake_case", codegen.getIdentifierNamingConvention());
    }

    @Test
    public void testGetIdentifierNamingConvention() {
        final PostgresqlSchemaCodegen codegen = new PostgresqlSchemaCodegen();
        Assert.assertSame("original", codegen.getIdentifierNamingConvention());
        codegen.setIdentifierNamingConvention("snake_case");
        Assert.assertSame("snake_case", codegen.getIdentifierNamingConvention());
    }

}
