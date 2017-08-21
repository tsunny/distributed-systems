package edu.buffalo.cse.cse486586.simpledht;

/**
 * Created by sunny on 3/25/16.
 */
public class Constants {

    static final int SERVER_PORT = 10000;

    static final String LOCAL = "@";

    static final String GLOBAL = "*";

    /* Messages */
    static final String M_JOIN = "M_JOIN";

    static final String M_RINGUPDATE = "M_RINGUPDATE";

    static final String M_INSERT = "M_INSERT";

    static final String M_QUERY = "M_QUERY";

    static final String M_QUERY_RESPONSE = "M_RESPONSE_QUERY";

    static final String M_GLOBAL_QUERY = "M_GLOBAL_QUERY";

    static final String M_GLOBAL_QUERY_RESPONSE = "M_RESPONSE_GLOBAL_QUERY";

    static final String M_GLOBAL_DELETE = "M_GLOBAL_DELETE";

    static final String M_GLOBAL_DELETE_RESPONSE = "M_RESPONSE_GLOBAL_DELETE";

    static final String CONTENT = "content";

    static final String AUTHORITY = "edu.buffalo.cse.cse486586.simpledht.provider";

    static final String DELIMITER = "|";

    static final String ESC_DELIMITER = "\\|";
}
