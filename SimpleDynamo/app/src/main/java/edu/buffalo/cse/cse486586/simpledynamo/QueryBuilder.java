package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by sunny on 4/7/16.
 */
public class QueryBuilder {

    public static String buildInsert(String tableName, String key, String value) {

        StringBuilder sb = new StringBuilder();


        sb.append("INSERT INTO ");

        sb.append(tableName);
        sb.append("(");
        sb.append(KeyValueContract.KeyValueEntry.KEY);
        sb.append(", ");
        sb.append(KeyValueContract.KeyValueEntry.VALUE);

        sb.append(")");
        sb.append(" VALUES ('");
        sb.append(key);
        sb.append("', '");
        sb.append(value);
        sb.append("')");

        return sb.toString();
    }

    public static String buildUpdate(String tableName, String key, String value) {

        StringBuilder sb = new StringBuilder();

        sb.append("UPDATE ");
        sb.append(tableName);

        sb.append(" SET ");
        sb.append(KeyValueContract.KeyValueEntry.VALUE);
        sb.append("='");
        sb.append(value);

        sb.append("' WHERE key='");
        sb.append(key);
        sb.append("'");

        return sb.toString();
    }


    public static String buildFind(String tableName, String key) {

        StringBuilder sb = new StringBuilder();

        sb.append("SELECT * FROM ");
        sb.append(tableName);
        sb.append(" WHERE key='");
        sb.append(key);
        sb.append("'");

        return sb.toString();
    }
}
