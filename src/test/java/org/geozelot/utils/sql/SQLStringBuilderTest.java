package org.geozelot.utils.sql;

import org.junit.jupiter.api.Test;

public class SQLStringBuilderTest {

    @Test
    public void statementIN_withParameter() {
        Object[] staticParameters = {1234, "Test"};

        SQLStringBuilder stringBuilder = new SQLStringBuilder()
                .WHERE()
                    .Column("column")
                    .IN("staticParameter1", "staticParameter2");

        try {
            stringBuilder
                    .setQueryParam("staticParameter1", staticParameters[0])
                    .setQueryParam("staticParameter2", staticParameters[1]);
            System.out.println("[Named parameter injection]: " + stringBuilder.getCompiled());

            stringBuilder
                    .clearQueryParams()
                    .setQueryParamsPositionally(staticParameters);
            System.out.println("[Ordinal parameter injection]: " + stringBuilder.getCompiled());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void callProcedure_withParameter() {
        Object[] staticParameters = {1234, "Test"};

        SQLStringBuilder stringBuilder = new SQLStringBuilder()
                .CALL()
                .VoidProcedure(
                        SQLStringBuilder.Identifier("SomeProcedure", "database", "schema"),
                        "staticParameter1", "staticParameter2"
                );

        try {
            stringBuilder
                    .setQueryParam("staticParameter1", staticParameters[0])
                    .setQueryParam("staticParameter2", staticParameters[1]);
            System.out.println("[Named parameter injection]: " + stringBuilder.getCompiled());

            stringBuilder
                    .clearQueryParams()
                    .setQueryParamsPositionally(staticParameters);
            System.out.println("[Ordinal parameter injection]: " + stringBuilder.getCompiled());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
