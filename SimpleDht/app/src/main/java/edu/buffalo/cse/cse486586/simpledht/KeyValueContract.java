package edu.buffalo.cse.cse486586.simpledht;

import android.provider.BaseColumns;

/**
 * Created by sunny on 2/11/16.
 * <p>
 * http://developer.android.com/training/basics/data-storage/databases.html
 * <p>
 * I have referred to the FeedReaderContract Example and followed the same pattern here.
 */
public final class KeyValueContract {

    public KeyValueContract() {

    }

    public static abstract class KeyValueEntry implements BaseColumns {
        public static final String DATABASE_NAME = "gm1";
        public static final String TABLE_NAME = "test";
        public static final String KEY = "key";
        public static final String VALUE = "value";
    }


}
