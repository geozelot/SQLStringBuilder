package org.geozelot.utils.sql;

public class SQLCompileException extends Exception {
    public SQLCompileException () {

    }

    public SQLCompileException (String message) {
        super (message);
    }

    public SQLCompileException (Throwable cause) {
        super (cause);
    }

    public SQLCompileException (String message, Throwable cause) {
        super (message, cause);
    }
}

