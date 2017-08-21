package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by sunny on 3/25/16.
 */
public class Constants {

    static final int SERVER_PORT = 10000;

    static final int TIME_OUT = 10000;

    static final String LOCAL = "@";

    static final String GLOBAL = "*";

    /* Messages */

    static final String PING = "PING";

    static final String ACK = "ACK";

    static final String M_INSERT = "M_0_INSERT";

    static final String M_INSERT_FAILURE = "M_1_INSERT_FAILURE";

    static final String M_QUERY = "M_2_QUERY";

    static final String M_QUERY_FAILURE = "M_3_QUERY_FAILURE";

    static final String M_DELETE = "M_4_DELETE";

    static final String M_REPLICA = "M_5_REPLICA";

    static final String M_REPLICA_FAILURE = "M_6_REPLICA_FAILURE";

    static final String M_REPLICA_RESPONSE = "M_7_RESPONSE_REPLICA";

    static final String M_QUERY_RESPONSE = "M_8_RESPONSE_QUERY";

    static final String M_QUERY_FAILURE_RESPONSE = "M_9_RESPONSE_QUERY_FAILURE";

    static final String M_DELETE_RESPONSE = "M_10_RESPONSE_DELETE";

    static final String M_DELETE_REPLICA = "M_11_DELETE_REPLICA";

    static final String M_GLOBAL_QUERY = "M_12_GLOBAL_QUERY";

    static final String M_GLOBAL_QUERY_RESPONSE = "M_13_GLOBAL_QUERY_RESPONSE";

    static final String M_GLOBAL_DELETE = "M_14_GLOBAL_DELETE";

    static final String M_GLOBAL_QUERY_FAILURE = "M_15_GLOBAL_QUERY_FAILURE";

    static final String M_GIVEME_FUEL = "M_16_FUEL";

    static final String M_GIVEME_FUEL_RESPONSE = "M_17_FUEL_RESPONSE";

    static final String M_INSERT_SUCCESS = "M_18_INSERT_SUCCESS";

    static final String M_RESET = "M_19_RESET";

    static final String M_DUMP = "M_20_DUMP";

    static final String M_DELETE_FAILURE = "M_21_DELETE_FAILURE";

    static final String M_DELETE_REPLICA_FAILURE = "M_22_DELETE_REPLICA_FAILURE";

    static final String M_DELETE_DUMP = "M_23_DELETE_DUMP";

    static final String M_GLOBAL_DELETE_FAILURE = "M_24_GLOBAL_DELETE_FAILURE";

    static final String CONTENT = "content";

    static final String AUTHORITY = "edu.buffalo.cse.cse486586.simpledynamo.provider";

    static final String DELIMITER = "|";

    static final String ESC_DELIMITER = "\\|";

    static final String NOROW = "NOROW";

    static final String REPLICATION_INCOMPLETE = "REPLICATION_INCOMPLETE";

    static final String REPLICATION_COMPLETE = "REPLICATION_COMPLETE";

    static final String YES_STORE = "Y";

    static final String NO_STORE = "N";

    static final String LOG_PREFIX = "SUNNY";

}
