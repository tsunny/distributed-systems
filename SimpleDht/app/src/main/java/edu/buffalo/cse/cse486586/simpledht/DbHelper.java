package edu.buffalo.cse.cse486586.simpledht;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

/**
 * Created by sunny on 2/11/16.
 */
public class DbHelper extends SQLiteOpenHelper {

    private static DbHelper instance;

    String SCHEMA_STRING = "CREATE TABLE IF NOT EXISTS " + KeyValueContract.KeyValueEntry.TABLE_NAME +
            "(key CHAR(50) PRIMARY KEY NOT NULL, " +
            "value CHAR(50));";

    /**
     * I have referred to the below article and I really liked the singleton pattern and decided to use it.
     * http://www.androiddesignpatterns.com/2012/05/correctly-managing-your-sqlite-database.html
     *
     * @param context
     * @return
     */
    public static synchronized DbHelper getInstance(Context context) {

        if (instance == null) {
            instance = new DbHelper(context);
        }
        return instance;
    }


    public DbHelper(Context context) {
        super(context, KeyValueContract.KeyValueEntry.DATABASE_NAME, null, 1);
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        db.execSQL(SCHEMA_STRING);
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {

    }
}