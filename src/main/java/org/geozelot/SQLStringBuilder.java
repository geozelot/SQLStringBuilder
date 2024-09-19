package org.geozelot;

import java.util.*;

public class SQLStringBuilder {
    /**
     * ENUM for query part control flow;
     * As we're using the actual ENUM values to generate the
     * respective query statement keywords, we have to override
     * stringification of two-word keywords
     */
    private enum QUERY_BLOCK {
        WITH,
        SELECT,
        SELECT_DISTINCT {
            @Override
            public String toString() {
                return "SELECT DISTINCT";
            }
        },
        FROM,
        WHERE,
        GROUP_BY {
            @Override
            public String toString() {
                return "GROUP BY";
            }
        },
        HAVING,
        ORDER_BY {
            @Override
            public String toString() {
                return "ORDER BY";
            }
        },
        OFFSET,
        LIMIT
    }

    private final Map<QUERY_BLOCK, StringJoiner> queryBlocks = new HashMap<QUERY_BLOCK, StringJoiner>();
    private QUERY_BLOCK currentBlock;

    private int parameterCount = 0;
    private int currentParameterOrdinal = 0;
    private ArrayList<String> parameterInjections;
    private Map<String, ArrayList<Integer>> parameterReferences;

    private String rawStatement = "";
    private String compiledStatement = "";

    private Boolean needsCompile = true;


// CONSTRUCTORS

    /**
     * Currently the default one only
     */
    public SQLStringBuilder() {}


// INTERNAL UTILITY METHODS

    /**
     * Some string operations leave artifacts that are just easier to remove
     * in a post-processing step; replaceAll is generally a bad choice, but
     * since this is only used during stringification of the statement, we
     * can afford a little less performance here...
     */
    private static String sanitizeQuery(String queryString) {
        return queryString
                .replaceAll(" , DESC", " DESC")
                .replaceAll(" , ASC", " ASC");
    }

    /**
     * Escape special chars in strings
     */
    private static String escapeChars(String input) {
        return input
                .replaceAll("'", "\\\\'")
                .replaceAll("\"", "\\\\\"");
    }

    /**
     * Prettify parameter wildcards for raw string representation
     */
    private static String prettifyQuery(String queryString) {
        return queryString
                .replaceAll("\\$\\$", "\\$")
                .replaceAll("\\$\\?", "\\$")
                .replaceAll(" , ", ", ")
                .replaceAll("\\( ", "(")
                .replaceAll(" \\)", ")");
    }

    /**
     * Wrap a given string in parentheses
     */
    private static String wrapParens(String unenclosed) {
        return String.format("( %s )", unenclosed);
    }

    /**
     * Wrap a given string in single quotes, if not already wrapped
     */
    private static String wrapSingleQuote(String unwrapped) {
        return unwrapped.startsWith("'") && unwrapped.endsWith("'") ? unwrapped : String.format("'%s'", unwrapped);
    }

    /**
     * Wrap a given string in double quotes, if not already wrapped
     */
    private static String wrapDoubleQuote(String unquoted) {
        return (unquoted.startsWith("\"") && unquoted.endsWith("\"")) || unquoted.equals("*") ? unquoted : String.format("\"%s\"", unquoted);
    }

    /**
     * Wrap a qualified identifier in double quotes, if not already wrapped
     */
    private static String wrapDoubleQuoteAndQualify(String qualify, String identifier) {
        return String.format("%s.%s", wrapDoubleQuote(qualify), wrapDoubleQuote(identifier));
    }

    /**
     * Wrap parameters according to their type, being either ordinal or named
     */
    private static String wrapOrdinalParameter(Object param) {
        return String.format("$$%s", param.toString());
    }
    private static String wrapNamedParameter(Object param) {
        return String.format("$?%s", param.toString());
    }

    /**
     * Add a given SQL alias as prefix to a given identifier
     */
    private static String addAliasPrefix(String identifier, String alias) {
        return alias.isEmpty() ? identifier : String.format("%s AS %s", wrapDoubleQuote(alias), identifier);
    }
    /**
     * Add a given SQL alias as suffix to given identifier
     */
    private static String addAliasSuffix(String identifier, String alias) {
        return alias.isEmpty() ? identifier : String.format("%s AS %s", identifier, wrapDoubleQuote(alias));
    }

    /**
     * Add an explicit CAST to a given type for a given value
     */
    private static String addCast(String value, String type) {
        return String.format("CAST( %s AS %s )", value, type);
    }


    /**
     * Identify if a string part is a parameter wildcard
     */
    private static Boolean isParameter(String part) {
        return part.startsWith("$$") || part.startsWith("$?");
    }
    /**
     * Identify if a string part is an ordinal parameter wildcard
     */
    private static Boolean isOrdinalParameter(String part) {
        return part.startsWith("$$");
    }

    /**
     * Increment ordinal number of an ordinal parameter wildcard
     */
    private static String incrementOrdinalParameter(String part, Integer increment) {
        Integer ordinal = Integer.valueOf(part.replaceAll("\\$", ""));

        return wrapOrdinalParameter(ordinal + increment);
    }

    /**
     * Initiate a new SQL block literal joiner and set to current block
     */
    private SQLStringBuilder blockPut(QUERY_BLOCK block, String delimiter) {
        this.currentBlock = block;
        this.queryBlocks.put(
                this.currentBlock,
                new StringJoiner(delimiter)
        );

        return this;
    }

    /**
     * Add literal to current SQL block
     */
    private SQLStringBuilder blockAdd(String literal) {
        this.queryBlocks.get(this.currentBlock)
                .add(literal);

        return this;
    }

    /**
     * Add multiple literals to current SQL block
     */
    private SQLStringBuilder blockAdd(String... literals) {
        for (String literal : literals) {
            this.blockAdd(literal);
        }

        return this;
    }

    /**
     * Add multiple literals to specified SQL block
     */
    private SQLStringBuilder blockAdd(QUERY_BLOCK block, String... literals) {
        this.currentBlock = block;

        return this.blockAdd(literals);
    }

    /**
     * Parse a given SQLStringBuilder instance into this instance as subquery;
     * Migrates all existing parameters into this instance and raps the resulting literal in parentheses
     */
    private String parseSubQuery(SQLStringBuilder subQuery) {
        String rawSubQuery = subQuery.toString();

        if (subQuery.parameterInjections != null) {
            if (this.parameterInjections == null) {
                this.parameterInjections = new ArrayList<>();
                this.parameterReferences = new HashMap<>();
            }

            String[] rawSubQueryParts = rawSubQuery.split(" ");

            for (int i = 0; i < rawSubQueryParts.length; i++) {
                if (isOrdinalParameter(rawSubQueryParts[i])) {
                    rawSubQueryParts[i] = incrementOrdinalParameter(rawSubQueryParts[i], this.currentParameterOrdinal);
                }
            }

            for (String reference : subQuery.parameterReferences.keySet()) {
                ArrayList<Integer> positions = subQuery.parameterReferences.get(reference);
                positions.replaceAll(position -> position + this.parameterCount);

                if (isOrdinalParameter(reference)) {
                    reference = incrementOrdinalParameter(reference, this.currentParameterOrdinal);
                }

                this.parameterReferences.put(reference, positions);
            }

            this.parameterInjections.addAll(subQuery.parameterInjections);
            this.parameterCount += subQuery.parameterCount;
            this.currentParameterOrdinal += subQuery.currentParameterOrdinal;

            rawSubQuery = String.join(" ", rawSubQueryParts);
        }

        return rawSubQuery;
    }

    /**
     * Build SQL string from statement blocks
     */
    private void build() {
        StringJoiner rawBuilder = new StringJoiner(" ");
        for (QUERY_BLOCK block : QUERY_BLOCK.values()) {
            if (this.queryBlocks.containsKey(block)) {
                rawBuilder.add(block.toString());
                rawBuilder.merge(this.queryBlocks.get(block));
            }
        }

        this.rawStatement = sanitizeQuery(rawBuilder.toString());
    }

    /**
     * Compile raw SQL string with parameter injections
     */
    private void compile() throws SQLCompileException {
        if (this.rawStatement.isBlank()) this.build();

        this.compiledStatement = this.rawStatement;
        if (!this.parameterInjections.isEmpty()) {
            String[] compiledBuilder = this.rawStatement.split(" ");

            int currentParameterPostion = -1;
            for (int i = 0; i < compiledBuilder.length; i++) {
                if (isParameter(compiledBuilder[i])) {
                    currentParameterPostion++;

                    if (currentParameterPostion > this.parameterCount) {
                        throw new SQLCompileException("Parameter injection position " + currentParameterPostion + " out of bounds with size " + this.parameterCount);
                    }

                    String injectionValue = this.parameterInjections.get(currentParameterPostion);
                    if (injectionValue == null) {
                        throw new SQLCompileException("No injection value found for parameter " + prettifyQuery(compiledBuilder[i]));
                    }

                    compiledBuilder[i] = injectionValue;
                }
            }

            this.compiledStatement = String.join(" ", compiledBuilder);
        }

        this.needsCompile = false;
    }



// PUBLIC HELPER METHODS

    /**
     * Helper method to create a valid (double-quoted) SQL identifier and optional qualifiers;
     * qualifiers are prepended to the identifier in order given;
     * uses Object.toString() method
     */
    public static String Identifier(Object identifier, Object... qualifiers) {
        StringJoiner qualifiedIdentifier = new StringJoiner(".");
        for (Object qualifier : qualifiers) {
            qualifiedIdentifier.add(wrapDoubleQuote(escapeChars(qualifier.toString())));
        }

        return qualifiedIdentifier.add(wrapDoubleQuote(escapeChars(identifier.toString()))).toString();
    }

    /**
     * Helper method to create a valid (single-quoted) SQL varchar (TEXT) value from a given Object;
     * uses Object.toString() method.
     */
    public static String Varchar(Object identifier) {
        return addCast(wrapSingleQuote(escapeChars(identifier.toString())), "TEXT");
    }

    /**
     * Wrap a given value into an explicit CAST to a given type
     */
    public static String Cast(Object value, String type) {
        return addCast(value.toString(), type);
    }

    /**
     * Create a SQL block wrapped in parentheses
     */
    public static String Block(Object block) {
        return wrapParens(block.toString());
    }



    // MAIN API: PRIMARY STATEMENT INITIALIZERS
    public SQLStringBuilder WITH() {
        return this.blockPut(QUERY_BLOCK.WITH, " , ");
    }

    public SQLStringBuilder CTE(SQLStringBuilder cte, String alias) {
        return this.blockAdd(addAliasPrefix(wrapParens(this.parseSubQuery(cte)), alias));
    }

    public SQLStringBuilder SELECT() {
        return this.blockPut(QUERY_BLOCK.SELECT, " , ");
    }

    public SQLStringBuilder SELECT_DISTINCT() {
        return this.blockPut(QUERY_BLOCK.SELECT_DISTINCT, " , ");
    }

    public SQLStringBuilder FROM() {
        return this.blockPut(QUERY_BLOCK.FROM, " ");
    }

    public SQLStringBuilder INNER_JOIN() {
        return this.blockAdd(QUERY_BLOCK.FROM, "INNER JOIN");
    }

    public SQLStringBuilder LEFT_JOIN() {
        return this.blockAdd(QUERY_BLOCK.FROM, "LEFT JOIN");
    }

    public SQLStringBuilder CROSS_JOIN() {
        return this.blockAdd(QUERY_BLOCK.FROM, "CROSS JOIN");
    }

    public SQLStringBuilder WHERE() {
        return this.blockPut(QUERY_BLOCK.WHERE, " ");
    }

    public SQLStringBuilder GROUP_BY() {
        return this.blockPut(QUERY_BLOCK.GROUP_BY, " , ");
    }

    public SQLStringBuilder ORDER_BY() {
        return this.blockPut(QUERY_BLOCK.ORDER_BY, " , ");
    }



// MAIN API: SECONDARY STATEMENT SUPPORT INITIALIZERS

    public SQLStringBuilder ON() {
        return this.blockAdd(QUERY_BLOCK.FROM, "ON");
    }

    public SQLStringBuilder USING(String... columns) {
        this.blockAdd(QUERY_BLOCK.FROM, "USING", "(");
        for (int i = 0; i < columns.length; i++) {
            this.blockAdd(QUERY_BLOCK.FROM, columns[i]);
            if (i <  columns.length - 1) this.blockAdd(",");
        }

        return this.blockAdd(QUERY_BLOCK.FROM, ")");
    }

    public SQLStringBuilder HAVING() {
        return this.blockPut(QUERY_BLOCK.HAVING, " ");
    }



// MAIN API: LOGICAL/SET OPERATORS

    public SQLStringBuilder NOT() {
        return this.blockAdd("NOT");
    }

    public SQLStringBuilder AND() {
        return this.blockAdd("AND");
    }

    public SQLStringBuilder OR() {
        return this.blockAdd("OR");
    }

    public SQLStringBuilder BETWEEN() {
        return this.blockAdd("BETWEEN");
    }

    public SQLStringBuilder EXISTS(SQLStringBuilder subQuery) {
        return this.blockAdd("EXISTS").SubQuery(subQuery, "");
    }

    public SQLStringBuilder IN(SQLStringBuilder subQuery) {
        return this.blockAdd("IN").SubQuery(subQuery, "");
    }

    public SQLStringBuilder IN(Integer parameterCount) {
        this.blockAdd("IN", "(");
        for (int i = 0; i < parameterCount; i++) {
            this.QueryParam();
            if (i < parameterCount - 1) this.blockAdd(",");
        }

        return this.blockAdd(")");
    }

    public SQLStringBuilder IN(Object... parameterNames) {
        this.blockAdd("IN", "(");
        for (int i = 0; i < parameterNames.length; i++) {
            this.QueryParam(parameterNames[i]);
            if (i < parameterNames.length - 1) this.blockAdd(",");
        }
        return this.blockAdd(")");
    }

    public SQLStringBuilder LIKE() {
        return this.blockAdd("LIKE");
    }


// MAIN API: MATHEMATICAL OPERATORS

    public SQLStringBuilder Equals() {
        return this.blockAdd("=");
    }

    public SQLStringBuilder GreaterThan() {
        return this.blockAdd(">");
    }

    public SQLStringBuilder GreaterThanOrEquals() {
        return this.blockAdd(">=");
    }

    public SQLStringBuilder LessThan() {
        return this.blockAdd("<");
    }

    public SQLStringBuilder LessThanOrEquals() {
        return this.blockAdd("<=");
    }



// MAIN API: NULL COMPARISON

    public SQLStringBuilder IS_NULL() {
        return this.blockAdd("IS NULL");
    }

    public SQLStringBuilder IS_NOT_NULL() {
        return this.blockAdd("IS NOT NULL");
    }


// MAIN API: ORDER OPERATORS

    public SQLStringBuilder ASC() {
        return this.blockAdd(QUERY_BLOCK.ORDER_BY, "ASC");
    }

    public SQLStringBuilder DESC() {
        return this.blockAdd(QUERY_BLOCK.ORDER_BY, "DESC");
    }

    public SQLStringBuilder OFFSET(Integer offset) {
        return this.blockPut(QUERY_BLOCK.OFFSET, " ")
                .blockAdd(offset.toString());
    }

    public SQLStringBuilder LIMIT(Integer limit) {
        return this.blockPut(QUERY_BLOCK.LIMIT, " ")
                .blockAdd(limit.toString());
    }



// MAIN API: VALUE INJECTION

    /**
     * Add a scalar value to the query string; will properly escape string literals
     */
    public SQLStringBuilder Scalar(Object value) {
        return this.blockAdd(value instanceof String ? wrapSingleQuote(escapeChars(value.toString())) : value.toString());
    }

    /**
     * Specifically add an ordinal value to the query string
     */
    public SQLStringBuilder Ordinal(Number ordinal) {
        return this.blockAdd(ordinal.toString());
    }



// MAIN API: OBJECT INJECTION */

    /**
     * Add an unqualified column identifier to the query string
     */
    public SQLStringBuilder Column(String column) {
        return this.blockAdd(wrapDoubleQuote(column));
    }

    /**
     * Add a table-qualified column identifier to the query string
     */
    public SQLStringBuilder Column(String qualifier, String column) {
        return this.blockAdd(wrapDoubleQuoteAndQualify(qualifier, column));
    }

    /**
     * Add a table-qualified column identifier with an alias to the query string
     */
    public SQLStringBuilder Column(String qualifier, String column, String alias) {
        return this.blockAdd(addAliasSuffix(wrapDoubleQuoteAndQualify(qualifier, column), alias));
    }

    /**
     * Add an unqualified column identifier to the query string and wrap inside a CAST
     */
    public SQLStringBuilder ColumnCast(String column, String type) {
        return this.blockAdd(addCast(wrapDoubleQuote(column), type));
    }

    /**
     * Add a table-qualified column identifier to the query string and wrap inside a CAST
     */
    public SQLStringBuilder ColumnCast(String qualifier, String column, String type) {
        return this.blockAdd(addCast(wrapDoubleQuoteAndQualify(qualifier, column), type));
    }

    /**
     * Add a table-qualified column identifier with an alias to the query string and wrap inside a CAST
     */
    public SQLStringBuilder ColumnCast(String qualifier, String column, String alias, String type) {
        return this.blockAdd(addAliasSuffix(addCast(wrapDoubleQuoteAndQualify(qualifier, column), type), alias));
    }

    /**
     * Add an unqualified *-selection to the query string
     */
    public SQLStringBuilder Columns() {
        return this.blockAdd("*");
    }

    /**
     * Add a table-qualified *-selection to the query string
     */
    public SQLStringBuilder Columns(String qualifier) {
        return this.blockAdd(wrapDoubleQuoteAndQualify(qualifier, "*"));
    }


    /**
     * Add a schema-qualified table identifier to the query string
     */
    public SQLStringBuilder Table(String schema, String table) {
        return this.Table(schema, table, table);
    }

    /**
     * Add a schema-qualified table identifier with an alias to the query string
     */
    public SQLStringBuilder Table(String schema, String table, String alias) {
        return this.blockAdd(addAliasSuffix(wrapDoubleQuoteAndQualify(schema, table), alias))
                ;
    }


    /**
     * Add a subquery with an alias to the query string; sub query is an instance of SQLStringBuilder
     * Query parameters in the subQuery will be merged into the current query; strictly ordinal parameters
     * will be incremented to come after any parameters present in the current query
     */
    public SQLStringBuilder SubQuery(SQLStringBuilder subQuery, String alias) {
        return this.blockAdd(addAliasSuffix(wrapParens(this.parseSubQuery(subQuery)), alias));
    }

    /**
     * Add a raw, unmodified string literal to the query string - for all things not supported by the API
     */
    public SQLStringBuilder Raw(String raw) {
        return this.blockAdd(raw);
    }



// MAIN API: FUNCTION CALL INJECTION

    /**
     * Add a scalar function with arguments and alias to the query string
     */
    public SQLStringBuilder ScalarFunction(String function, String alias, String... arguments) {
        StringJoiner functionCall = new StringJoiner("").add(function);

        StringJoiner argumentList = new StringJoiner(" , ", "( ", " )");
        for (String argument : arguments) {
            argumentList.add(argument);
        }

        return this.blockAdd(addAliasSuffix(functionCall.add(argumentList.toString()).toString(), alias));
    }

    /**
     * Add a scalar function with arguments and alias to the query string and wrao in a CAST
     */
    public SQLStringBuilder ScalarFunctionCast(String function, String alias, String type, String... arguments) {
        StringJoiner functionCall = new StringJoiner("").add(function);

        StringJoiner argumentList = new StringJoiner(" , ", "( ", " )");
        for (String argument : arguments) {
            argumentList.add(argument);
        }

        return this.blockAdd(addAliasSuffix(addCast(functionCall.add(argumentList.toString()).toString(), type), alias));
    }

    /**
     * Add an aggregate function with argument, optional DISTINCT and alias to the query string
     */
    public SQLStringBuilder AggFunction(String function, String alias, Boolean distinct, String argument) {
        String functionCall = String.format("%s( %s%s )", function, distinct ? "DISTINCT " : "", argument);
        return this.blockAdd(addAliasSuffix(functionCall, alias));
    }

    /**
     * Add an aggregate function with argument, optional DISTINCT and alias to the query string and wrap in a CAST
     */
    public SQLStringBuilder AggFunctionCast(String function, String alias, String type, Boolean distinct, String argument) {
        String functionCall = String.format("%s( %s%s )", function, distinct ? "DISTINCT " : "", argument);
        return this.blockAdd(addAliasSuffix(addCast(functionCall, type), alias));
    }



// MAIN API: QUERY PARAMETER INJECTION

    /**
     * Add a plain, ordinal parameter wildcard - ordinals are incremented internally and can be referenced with setQueryParam
     */
    public SQLStringBuilder QueryParam() {
        this.currentParameterOrdinal++;

        return this.QueryParam(null);
    }

    /**
     * Add a named/specific ordinal parameter wildcard - specifically place a parameter wildcard addressable by given reference in setQueryParam
     */
    public SQLStringBuilder QueryParam(Object reference) {
        if (this.parameterInjections == null) {
            this.parameterInjections = new ArrayList<>();
            this.parameterReferences = new HashMap<>();
        }

        this.parameterCount++;
        this.parameterInjections.add(null);

        String parameterReference = reference == null ? wrapOrdinalParameter(this.currentParameterOrdinal) : wrapNamedParameter(reference);
        if (!this.parameterReferences.containsKey(parameterReference)) {
            this.parameterReferences.put(parameterReference, new ArrayList<>());
        }

        this.parameterReferences.get(parameterReference)
                .add(this.parameterCount-1);

        this.queryBlocks.get(this.currentBlock)
                .add(parameterReference);

        this.needsCompile = true;
        return this;
    }

    /**
     * Add an injection value for a stored parameter wildcard, addressed specifically by the given reference
     */
    public SQLStringBuilder setQueryParam(Object reference, Object value) throws SQLCompileException {
        String parameterReference = reference instanceof Integer ? wrapOrdinalParameter(reference) : wrapNamedParameter(reference);
        if (!this.parameterReferences.containsKey(parameterReference)) {
            throw new SQLCompileException("No parameter found with reference: " + reference);
        }

        for (Integer position : this.parameterReferences.get(parameterReference)) {
            this.parameterInjections.set(position, value.toString());
        }

        this.needsCompile = true;
        return this;
    }

    /**
     * Add a set of parameter injection values strictly by position within the SQL string literal - first wildcard in the string gets first value aso.
     * Clears out all stored injection values
     */
    public SQLStringBuilder setQueryParamsPositionally(Object... values) throws SQLCompileException {
        this.clearQueryParams();

        for (int i = 0; i < values.length && i <= this.parameterCount; i++) {
            this.parameterInjections.set(i, values[i].toString());
        }

        return this;
    }

    /**
     * Optionally remove all stored injection values
     */
    public SQLStringBuilder clearQueryParams() {
        this.parameterInjections.replaceAll(ignored -> null);

        this.needsCompile = true;
        return this;
    }



// MAIN API: COMPILING PARAMETERS AND CONSTRUCTING STATEMENT

    /**
     * Retrieve SQL string literal with all parameter values injected
     * Optionally prettify query string
     */
    public String getCompiled(Boolean pretty) throws SQLCompileException {
        if (this.needsCompile || this.compiledStatement.isBlank()) {
            this.compile();
        }

        return pretty ? prettifyQuery(this.compiledStatement) : this.compiledStatement;
    }

    public String getCompiled() throws SQLCompileException {
        return this.getCompiled(false);
    }

    /**
     * Retrieve the raw SQL string literal with parameter wildcards
     */
    public String getRaw(Boolean pretty) {
        if (this.rawStatement.isBlank()) {
            this.build();
        }

        return pretty ? prettifyQuery(this.rawStatement) : this.rawStatement;
    }

    public String getRaw() {
        return this.getRaw(true);
    }

    @Override
    public String toString() {
        if (this.rawStatement.isBlank()) {
            this.build();
        }

        return this.rawStatement;
    }


    // Check out how this works
    public static void main(String[] args) throws SQLCompileException {
        SQLStringBuilder cte1 = new SQLStringBuilder();
        SQLStringBuilder cte2 = new SQLStringBuilder();

        SQLStringBuilder subQuery = new SQLStringBuilder();
        SQLStringBuilder query = new SQLStringBuilder();

        // there's support for sub queries
        subQuery
                .SELECT()
                .Columns()
                .FROM()
                .Table("public", "t6")
                .WHERE()
                .Column("t6", "val").Equals().QueryParam()
        ;

        cte1
                .SELECT()
                .Columns()
                .FROM()
                .Table("public", "t6")
                .WHERE()
                .Column("t6", "val").NOT().IN(2)
        ;

        cte2
                .SELECT()
                .Columns()
                .FROM()
                .Table("public", "t7")
                .WHERE()
                .NOT().EXISTS(
                        new SQLStringBuilder()
                                .SELECT()
                                .Scalar(1)
                                .FROM()
                                .Table("public", "t7")
                                .WHERE().Column("val").LIKE().Scalar("%THIS%")
                )
        ;

        query
                .WITH()
                .CTE(cte1, "cte1")
                .CTE(cte2, "cte2")
                .SELECT()
                .Columns()
                .Columns("t1")
                .Column("t1", "id")
                .Column("t2", "key")
                .ScalarFunctionCast(
                        "REPLACE", "new_val", "TEXT",
                        SQLStringBuilder.Identifier( "val", "t2"),
                        SQLStringBuilder.Varchar("hello"),
                        SQLStringBuilder.Varchar("Hello")
                )
                .AggFunction(
                        "ARRAY_AGG", "cnt", true,
                        SQLStringBuilder.Identifier("val", "t2")
                )
                .FROM()
                .Table("public", "test1", "t1")
                .LEFT_JOIN()
                .Table("public", "test2", "t2")
                .ON()
                .Column("t1", "fkey").Equals().ColumnCast("t2", "key", "DOUBLE PRECISION")
                .AND()
                .Column("t2", "val").GreaterThanOrEquals().QueryParam()
                .INNER_JOIN()
                .SubQuery(subQuery, "t3")
                .USING("id", "fkey")
                .WHERE()
                .NOT().Column("t1", "key")
                .BETWEEN().QueryParam("LOWER")
                .AND().QueryParam("UPPER")
                .AND()
                .Column("t1", "val").IS_NOT_NULL()
                .GROUP_BY()
                .Ordinal(1).Column("new_val")
                .HAVING()
                .AggFunction("COUNT", "", false, "*").GreaterThan().QueryParam()
                .ORDER_BY()
                .Column("t1", "id").DESC()
                .Column("test3", "seq").ASC()
                .OFFSET(12314)
                .LIMIT(1000)
        ;

        System.out.println(query.getRaw());
        query.setQueryParamsPositionally(1, 2, 3, SQLStringBuilder.Varchar("THIS IS ' A \" PARAMETER"), 3.0, 10.0, 1234);
        System.out.println(query.getCompiled());

/*
        query.clearQueryParams()
            .setQueryParam(1, 6.0)
            .setQueryParam(3, SQLStringBuilder.Varchar("THIS IS ' ANOTHER \" PARAMETER"))
            .setQueryParam(2, 100.0)
            .setQueryParam("KEY", 4567);

        System.out.println(query.getCompiled());
        System.out.println(query.getRaw());

 */
    }
}