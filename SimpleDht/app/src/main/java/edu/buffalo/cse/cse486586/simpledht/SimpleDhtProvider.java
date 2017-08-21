package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

    /* Constants */
    private static final String TAG = SimpleDhtProvider.class.getSimpleName() + " | [DP] ";
    private static final String S_TAG = ServerTask.class.getSimpleName() + " | [SP] ";
    private static final String C_TAG = ClientTask.class.getSimpleName() + " | [CP] ";

    /* AVD specific variables */
    static Uri uri = null;
    private static String myPort = null;
    private static String myHash = null;
    private static String avd0 = "5554";
    private static ContentResolver contentResolver = null;

    /* Ring Data Structures */
    private static List<String> ring = new ArrayList<String>();
    private String[] neighborArr = new String[3];
    private static Map<String, String> reverseLookup = new HashMap<String, String>();

    /* Query Data Structures */
    private Map<String, String> queryResultMap = new HashMap<String, String>();

    /* synchronization constructs */
    private Semaphore querySem = new Semaphore(0, true);
    private Semaphore deleteSem = new Semaphore(0, true);


    @Override
    public boolean onCreate() {

        // set the content resolver
        contentResolver = getContext().getContentResolver();

        // build URI
        Uri.Builder builder = new Uri.Builder();
        builder.scheme(Constants.CONTENT);
        builder.authority(Constants.AUTHORITY);
        uri = builder.build();

        /* set myPort*/
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr)));

        /* Create server socket */
        try {

            ServerSocket serverSocket = new ServerSocket(Constants.SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

        } catch (Exception e) {

            Log.e(TAG, "Error when creating ServerSocket : " + e.getMessage());
            return false;
        }

        /* Hash my port */
        try {

            myHash = genHash(myPort);

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        // initially there will be no neighbors
        neighborArr[0] = myPort;
        neighborArr[1] = myPort;
        neighborArr[2] = myPort;

        // invoke node join and register
        String nodeJoinMessage = Constants.M_JOIN + Constants.DELIMITER + myPort;

        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                nodeJoinMessage, GlobalRepo.getPortNumber(avd0));

        return false;
    }


    @Override
    public Uri insert(Uri uri, ContentValues values) {

        Log.d(TAG, "IN insert");

        SQLiteDatabase sqLiteDatabase = DbHelper.getInstance(getContext()).getWritableDatabase();

        if (values.size() > 0) {

            String key = (String) values.get(KeyValueContract.KeyValueEntry.KEY);
            String value = (String) values.get(KeyValueContract.KeyValueEntry.VALUE);

            if (areYouMine(key)) {

                String findQuery = "SELECT * FROM " + KeyValueContract.KeyValueEntry.TABLE_NAME + " WHERE key='" + key +
                        "'";
                Log.d(TAG, findQuery);
                Cursor cursor = sqLiteDatabase.rawQuery(findQuery, null);

                if (cursor.moveToFirst()) {

                    int keyIndex = cursor.getColumnIndex(KeyValueContract.KeyValueEntry.KEY);
                    int columnIndex = cursor.getColumnIndex(KeyValueContract.KeyValueEntry.VALUE);
                    cursor.getString(keyIndex);
                    cursor.getString(columnIndex);

                    String updateStatement = "UPDATE " + KeyValueContract.KeyValueEntry.TABLE_NAME + " SET " +
                            KeyValueContract.KeyValueEntry.VALUE + "='" + value + "' WHERE key='" + key + "'";
                    Log.d(TAG, updateStatement);
                    sqLiteDatabase.rawQuery(updateStatement, null);

                } else {

                    String insertStatement = "INSERT INTO " + KeyValueContract.KeyValueEntry.TABLE_NAME + "(" +
                            KeyValueContract.KeyValueEntry.KEY + ", "
                            + KeyValueContract.KeyValueEntry.VALUE +
                            ")" + " VALUES ('" + key + "', '" + value + "')";
                    Log.d(TAG, insertStatement);

                    sqLiteDatabase.insert(KeyValueContract.KeyValueEntry.TABLE_NAME, null, values);
                }

                cursor.close();

            } else {

                // send it to successor

                String successor = neighborArr[2];
                String insertMessage = Constants.M_INSERT + Constants.DELIMITER + key + Constants.DELIMITER + value;

                Log.d(TAG, "Forwarding key to successor : " + insertMessage);

                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                        insertMessage, GlobalRepo.getPortNumber(successor));

            }
        }

        Log.d(TAG, "OUT insert");

        return uri;
    }


    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {

        Log.d(TAG, "IN query");


        String extractQuery = null;
        String key = selection;
        Cursor cursor = null;

        if (key.equals(Constants.LOCAL)) {

            // local

            extractQuery = "SELECT * FROM " + KeyValueContract.KeyValueEntry.TABLE_NAME;

            SQLiteOpenHelper helper = DbHelper.getInstance(getContext());
            SQLiteDatabase readableDatabase = helper.getReadableDatabase();
            cursor = readableDatabase.rawQuery(extractQuery, null);

        } else if (key.equals(Constants.GLOBAL)) {

            // global

            queryResultMap = new HashMap<String, String>();


            String predecessor = neighborArr[0];
            String successor = neighborArr[2];

            if (successor.equals(myPort) && predecessor.equals(myPort)) {

                extractQuery = "SELECT * FROM " + KeyValueContract.KeyValueEntry.TABLE_NAME;

                SQLiteOpenHelper helper = DbHelper.getInstance(getContext());
                SQLiteDatabase readableDatabase = helper.getReadableDatabase();
                cursor = readableDatabase.rawQuery(extractQuery, null);

            } else {


                // create a global message and wait
                String globalQueryMessage = Constants.M_GLOBAL_QUERY + Constants.DELIMITER + myPort;

                Log.d(TAG, "Sending Global query to successor : " + globalQueryMessage);

                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                        globalQueryMessage, GlobalRepo.getPortNumber(successor));

                Log.d(TAG, "Global Query method will go to sleep");
                try {
                    querySem.acquire();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                Log.d(TAG, "Global Query method resuming execution");

                Log.d(TAG, "Received total number of messages : " + queryResultMap.size());

                Log.d(TAG, "Creating a cursor object");

                MatrixCursor matrixCursor = new MatrixCursor(new String[]{KeyValueContract.KeyValueEntry.KEY,
                        KeyValueContract.KeyValueEntry.VALUE});


                for (Map.Entry<String, String> entry : queryResultMap.entrySet()) {

                    matrixCursor.addRow(new String[]{entry.getKey(), entry.getValue()});
                }

                Log.d(TAG, "Done creating a cursor object");

                Log.d(TAG, "Extracting from local database");

                extractQuery = "SELECT * FROM " + KeyValueContract.KeyValueEntry.TABLE_NAME;

                SQLiteOpenHelper helper = DbHelper.getInstance(getContext());
                SQLiteDatabase readableDatabase = helper.getReadableDatabase();
                cursor = readableDatabase.rawQuery(extractQuery, null);

                Log.d(TAG, "Done Extracting from local database");

                MergeCursor mergeCursor = new MergeCursor(new Cursor[]{matrixCursor, cursor});
                cursor = mergeCursor;

                Log.d(TAG, "Done merging cursors");
            }


        } else {


            Log.d(TAG, "Doing single query");

            // single query

            // check if the key is belongs to me
            if (areYouMine(key)) {

                extractQuery = "SELECT * FROM " + KeyValueContract.KeyValueEntry.TABLE_NAME + " WHERE key='" +
                        key + "'";

                Log.d(TAG, extractQuery);

                SQLiteOpenHelper helper = DbHelper.getInstance(getContext());
                SQLiteDatabase readableDatabase = helper.getReadableDatabase();
                cursor = readableDatabase.rawQuery(extractQuery, null);

            } else {

                // send it to successor
                String successor = neighborArr[2];
                String queryMessage = Constants.M_QUERY + Constants.DELIMITER + key + Constants.DELIMITER + myPort;

                Log.d(TAG, "Sending query to successor : " + queryMessage);

                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                        queryMessage, GlobalRepo.getPortNumber(successor));

                // Wait here (okay boss!)
                try {
                    querySem.acquire();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                Log.d(TAG, "Query method resuming execution");

                /* Reference: http://chariotsolutions.com/blog/post/android-advanced-cursors/ */

                // Get the value from the global map and return a cursor
                String value = queryResultMap.get(key);

                Log.d(TAG, "Query result for Key : " + key + " , Value : " + value);

                MatrixCursor matrixCursor = new MatrixCursor(new String[]{KeyValueContract.KeyValueEntry.KEY,
                        KeyValueContract.KeyValueEntry.VALUE});

                matrixCursor.addRow(new String[]{key, value});

                cursor = matrixCursor;
            }
        }

        Log.d(TAG, "OUT query");
        return cursor;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        /* References : http://developer.android.com/training/basics/data-storage/databases.html#DeleteDbRow
        * http://stackoverflow.com/questions/7510219/deleting-row-in-sqlite-in-android
        * http://stackoverflow.com/questions/3094444/delete-all-rows-from-a-table-throws-nullpointer
        */

        Log.d(TAG, "IN delete");

        String key = selection;
        int delete = -1;

        if (key.equals(Constants.LOCAL)) {

            Log.d(TAG, "delete | LOCAL");

            SQLiteOpenHelper helper = DbHelper.getInstance(getContext());
            SQLiteDatabase writableDatabase = helper.getWritableDatabase();

            delete = writableDatabase.delete(KeyValueContract.KeyValueEntry.TABLE_NAME, null, null);

            writableDatabase.close();


        } else if (key.equals(Constants.GLOBAL)) {

            Log.d(TAG, "delete | GLOBAL");


            String predecessor = neighborArr[0];
            String successor = neighborArr[2];

            if (successor.equals(myPort) && predecessor.equals(myPort)) {

                Log.d(TAG, "delete | Only one node so deleting locally");

                SQLiteOpenHelper helper = DbHelper.getInstance(getContext());
                SQLiteDatabase writableDatabase = helper.getWritableDatabase();

                delete = writableDatabase.delete(KeyValueContract.KeyValueEntry.TABLE_NAME, null, null);

                writableDatabase.close();
                
            } else {


                String globalDeleteMessage = Constants.M_GLOBAL_DELETE + Constants.DELIMITER + myPort;

                Log.d(TAG, "Sending Global Delete to successor : " + globalDeleteMessage);

                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                        globalDeleteMessage, GlobalRepo.getPortNumber(successor));

                Log.d(TAG, "Global Delete method will go to sleep");
                try {
                    deleteSem.acquire();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                Log.d(TAG, "Global Delete method resuming execution");


                SQLiteOpenHelper helper = DbHelper.getInstance(getContext());
                SQLiteDatabase writableDatabase = helper.getWritableDatabase();
                delete = writableDatabase.delete(KeyValueContract.KeyValueEntry.TABLE_NAME, null, null);

                writableDatabase.close();

            }

        } else {

            Log.d(TAG, "delete | SINGLE ROW");

            SQLiteOpenHelper helper = DbHelper.getInstance(getContext());
            SQLiteDatabase writableDatabase = helper.getWritableDatabase();

            delete = writableDatabase.delete(KeyValueContract.KeyValueEntry.TABLE_NAME,
                    KeyValueContract.KeyValueEntry.KEY + "='" + key + "'", null);


            writableDatabase.close();
        }

        Log.d(TAG, "OUT delete");

        return delete;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }


    /**
     *
     */
    public class ServerTask extends AsyncTask<ServerSocket, Void, Void> {

        /**
         * @param sockets
         * @return
         */
        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            ServerSocket serverSocket = sockets[0];

            try {

                while (true) {

                    Socket clientSocket = serverSocket.accept();

                    InputStream inputStream = clientSocket.getInputStream();

                    BufferedReader bufferedReader = new BufferedReader(new
                            InputStreamReader(inputStream));

                    String message = bufferedReader.readLine();

                    // TODO : Add null check here

                    if (message.startsWith(Constants.M_JOIN)) {

                        // a node wants to join the ring
                        doJoin(message);

                    } else if (message.startsWith(Constants.M_RINGUPDATE)) {

                        // a node was added so update the ring
                        doRingUpdate(message);

                    } else if (message.startsWith(Constants.M_INSERT)) {

                        // see if this node has to be inserted
                        doInsert(message);

                    } else if (message.startsWith(Constants.M_QUERY)) {

                        // incoming query
                        doQuery(message);

                    } else if (message.startsWith(Constants.M_QUERY_RESPONSE)) {

                        // query response
                        doQueryResponse(message);

                    } else if (message.startsWith(Constants.M_GLOBAL_QUERY)) {

                        // global query
                        doGlobalQuery(message);

                    } else if (message.startsWith(Constants.M_GLOBAL_QUERY_RESPONSE)) {

                        // global query response
                        doGlobalQueryResponse(message);

                    } else if (message.startsWith(Constants.M_GLOBAL_DELETE)) {

                        // global query delete
                        doGlobalDelete(message);

                    } else if (message.startsWith(Constants.M_GLOBAL_DELETE_RESPONSE)) {

                        // global query delete response
                        doGlobalDeleteResponse(message);
                    }


                    inputStream.close();
                }

            } catch (IOException e) {
                Log.e(S_TAG, "Error in ServerTask : " + e.getMessage());
            }

            return null;
        }

        /**
         * @param message
         */
        private void doGlobalDeleteResponse(String message) {


            Log.d(S_TAG, "IN doGlobalDeleteResponse");

            Log.d(S_TAG, "doGlobalDeleteResponse | Global Delete Response : " + message);


            String[] split = message.split(Constants.ESC_DELIMITER);
            String source = split[1];

            if (myPort.equals(source)) {
                Log.d(S_TAG, "doGlobalDeleteResponse | Delete went around the ring. Waking up delete thread");
                querySem.release();
            }

            Log.d(S_TAG, "OUT doGlobalDeleteResponse");
        }

        /**
         * @param message
         */
        private void doGlobalDelete(String message) {

            Log.d(S_TAG, "IN doGlobalDelete");

            Log.d(S_TAG, "doGlobalDelete | Global Delete : " + message);

            String[] split = message.split(Constants.ESC_DELIMITER);
            String source = split[1];

            contentResolver.delete(uri, Constants.LOCAL, null);

            // Query the local database and send the response

            String successor = neighborArr[2];
            String globalDeleteResponse = Constants.M_GLOBAL_DELETE_RESPONSE + Constants.DELIMITER + successor;


            Log.d(S_TAG, "doGlobalDelete | Sending response back to : " + source);

            // Send the response back to the sender
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                    globalDeleteResponse, GlobalRepo.getPortNumber(source));

            Log.d(S_TAG, "doGlobalDelete | Forwarding Global Delete to successor : " + successor);

            // forward the message to the successor
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                    message, GlobalRepo.getPortNumber(successor));

            Log.d(S_TAG, "OUT doGlobalDelete");

        }

        /**
         * @param message
         */
        private void doGlobalQueryResponse(String message) {

            Log.d(S_TAG, "IN doGlobalQueryResponse");

            Log.d(S_TAG, "doGlobalQueryResponse | Global Query Response : " + message);


            String[] split = message.split(Constants.ESC_DELIMITER);
            String source = split[1];

            for (int i = 2; i < split.length; i = i + 2) {

                String key = split[i];
                String value = split[i + 1];

                queryResultMap.put(key, value);
            }

            if (myPort.equals(source)) {
                Log.d(S_TAG, "doGlobalQueryResponse | Received query results from all nodes. Waking up query thread");
                querySem.release();
            }


            Log.d(S_TAG, "OUT doGlobalQueryResponse");
        }

        /**
         * @param message
         */
        private void doGlobalQuery(String message) {

            Log.d(S_TAG, "IN doGlobalQuery");

            Log.d(S_TAG, "doGlobalQuery | Global Query : " + message);

            String[] split = message.split(Constants.ESC_DELIMITER);
            String source = split[1];

            // Query the local database and send the response

            String successor = neighborArr[2];

            StringBuilder gloablQueryResponse = new StringBuilder();
            gloablQueryResponse.append(Constants.M_GLOBAL_QUERY_RESPONSE + Constants.DELIMITER + successor);

            /*References : http://stackoverflow.com/questions/4920528/iterate-through-rows-from-sqlite-query */

            Cursor cursor = contentResolver.query(uri, null, Constants.LOCAL, null, null);

            cursor.moveToFirst();

            int keyIndex = cursor.getColumnIndex(KeyValueContract.KeyValueEntry.KEY);
            int valueIndex = cursor.getColumnIndex(KeyValueContract.KeyValueEntry.VALUE);

            while (!cursor.isAfterLast()) {

                String rowKey = cursor.getString(keyIndex);
                String value = cursor.getString(valueIndex);

                gloablQueryResponse.append(Constants.DELIMITER);
                gloablQueryResponse.append(rowKey);
                gloablQueryResponse.append(Constants.DELIMITER);
                gloablQueryResponse.append(value);

                cursor.moveToNext();
            }


            cursor.close();

            Log.d(S_TAG, "doGlobalQuery | Sending response back to : " + source);

            // Send the response back to the sender
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                    gloablQueryResponse.toString(), GlobalRepo.getPortNumber(source));

            Log.d(S_TAG, "doGlobalQuery | Forwarding Global Query to successor : " + successor);

            // forward the message to the successor
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                    message, GlobalRepo.getPortNumber(successor));

            Log.d(S_TAG, "OUT doGlobalQuery");
        }

        /**
         * @param message
         */
        private void doQueryResponse(String message) {

            Log.d(S_TAG, "IN doQueryResponse");

            Log.d(S_TAG, "doQueryResponse | Query Response : " + message);

            String[] split = message.split(Constants.ESC_DELIMITER);
            String key = split[1];
            String value = split[2];

            queryResultMap = new HashMap<String, String>();
            queryResultMap.put(key, value);


            Log.d(S_TAG, "doQueryResponse | Waking up query thread");
            querySem.release();


            Log.d(S_TAG, "Out doQueryResponse");
        }

        /**
         * @param message
         */
        private void doQuery(String message) {

            Log.d(S_TAG, "IN doQuery");

            Log.d(S_TAG, "doQuery | Query message : " + message);

            String[] split = message.split(Constants.ESC_DELIMITER);
            String key = split[1];
            String source = split[2];

            if (areYouMine(key)) {

                Log.d(S_TAG, "doQuery | Key is mine : " + key);

                // key belongs to mine. query and send back the result to the source.

                Cursor cursor = contentResolver.query(uri, null, key, null, null);
                if (cursor.isBeforeFirst()) {

                    if (cursor.moveToFirst()) {

                        int keyIndex = cursor.getColumnIndex(KeyValueContract.KeyValueEntry.KEY);
                        int valueIndex = cursor.getColumnIndex(KeyValueContract.KeyValueEntry.VALUE);

                        String rowKey = cursor.getString(keyIndex);
                        String value = cursor.getString(valueIndex);


                        Log.d(S_TAG, "doQuery | Sending result back to : " + source);

                        String queryResponseMessage = Constants.M_QUERY_RESPONSE + Constants.DELIMITER + rowKey +
                                Constants.DELIMITER + value;

                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                                queryResponseMessage, GlobalRepo.getPortNumber(source));
                    }
                }

            } else {

                // not mine.. forward
                String successor = neighborArr[2];

                Log.d(S_TAG, "doQuery | Key is NOT mine : " + key + " . Forwarding to my successor : " + successor);

                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                        message, GlobalRepo.getPortNumber(successor));
            }

            Log.d(S_TAG, "OUT doQuery");
        }

        /**
         * @param message
         */
        private void doInsert(String message) {

            Log.d(S_TAG, "IN doInsert");


            Log.d(S_TAG, "doInsert | Inserting message : " + message);

            String[] split = message.split(Constants.ESC_DELIMITER);
            String key = split[1];
            String value = split[2];

            ContentValues contentValues = new ContentValues();
            contentValues.put(KeyValueContract.KeyValueEntry.KEY, key);
            contentValues.put(KeyValueContract.KeyValueEntry.VALUE, value);

            contentResolver.insert(uri, contentValues);

            Log.d(S_TAG, "OUT doInsert");
        }

        /**
         * @param message
         */
        private void doRingUpdate(String message) {

            Log.d(S_TAG, "IN doRingUpdate");

            String[] split = message.split(Constants.ESC_DELIMITER);
            String pre = split[1];
            String suc = split[2];


            Log.d(S_TAG, "doRingUpdate | Old pre and suc : [" + neighborArr[0] + ", " + neighborArr[2] + "]");

            neighborArr[0] = pre;
            neighborArr[1] = myPort;
            neighborArr[2] = suc;

            Log.d(S_TAG, "doRingUpdate | New pre and suc : [" + pre + ", " + suc + "]");

            Log.d(S_TAG, "Out doRingUpdate");
        }

        /**
         * @param message
         */
        private void doJoin(String message) {

            Log.d(S_TAG, "IN doJoin");

            String[] split = message.split(Constants.ESC_DELIMITER);
            String node = split[1];
            String hashedNode = "";

            try {
                hashedNode = genHash(node);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }


            if (ring.contains(hashedNode)) {

                Log.d(S_TAG, "doJoin | Node is already present : " + node);

            } else {

                ring.add(hashedNode);
                reverseLookup.put(hashedNode, node);

                Collections.sort(ring);

                Log.d(S_TAG, "doJoin | Added a new node to the ring : " + node);

                int len = ring.size();
                StringBuilder sb = new StringBuilder();

                for (int i = 0; i < len; i++) {

                    // http://stackoverflow.com/questions/4403542/how-does-java-do-modulus-calculations-with-negative
                    // -numbers

                    int preIndex = (((i - 1) % len) + len) % len;
                    int sucIndex = (((i + 1) % len) + len) % len;

                    String pre = reverseLookup.get(ring.get(preIndex));
                    String suc = reverseLookup.get(ring.get(sucIndex));
                    String current = reverseLookup.get(ring.get(i));

                    sb.append(current);
                    sb.append(" " + Constants.DELIMITER + " ");

                    String ringUpdateMessage = Constants.M_RINGUPDATE + Constants.DELIMITER
                            + pre + Constants.DELIMITER + suc;

                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                            ringUpdateMessage, GlobalRepo.getPortNumber(current));
                }

                Log.d(S_TAG, "doJoin | Current ring : " + sb.toString());
            }

            Log.d(S_TAG, "OUT doJoin");
        }

    }

    public class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            try {

                String messageToBeSent = msgs[0];

                String remotePort = msgs[1];

                int emulatorPort = Integer.parseInt(remotePort) / 2;

                Socket socket = new Socket(InetAddress.getByAddress
                        (new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(remotePort));

                Log.d(C_TAG, "Sending to : " + emulatorPort + ". Message : " + messageToBeSent);

                OutputStream outputStream = socket.getOutputStream();
                outputStream.write(messageToBeSent.getBytes());

                socket.close();

            } catch (Exception e) {
                e.printStackTrace();
                Log.e(C_TAG, "Error in ClientTask : " + e.getMessage());
            }

            return null;
        }
    }

    /**
     * Just a funny name for the function ;)
     *
     * @param key
     * @return
     */
    private boolean areYouMine(String key) {

        Log.d(TAG, "IN areYouMine");

        String hashedKey = "";
        String preDecHash = "";

        try {

            hashedKey = genHash(key);
            preDecHash = genHash(neighborArr[0]);

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        Log.d(TAG, "HASHED_KEY : " + hashedKey + " PREDEC_HASH : " + preDecHash + " MY_HASH : " + myHash);


        // I am the only node in the ring. So you are mine
        if (preDecHash.compareTo(myHash) == 0) {
            return true;
        }

        // Key is greater than previous but less than current so it has to be me.. don't look for neighbors
        if ((hashedKey.compareTo(preDecHash) > 0 && hashedKey.compareTo(myHash) <= 0)) {
            return true;
        }

        // key is greater the predecessor and me and I am the lowest node in the ring.. Hurray I win
        if (hashedKey.compareTo(preDecHash) > 0 && hashedKey.compareTo(myHash) > 0
                && preDecHash.compareTo(myHash) > 0) {
            return true;
        }

        // key is less than my predecessor and it is less than me and I am the lowest node in the ring. I win.
        if (hashedKey.compareTo(preDecHash) < 0 && hashedKey.compareTo(myHash) < 0
                && preDecHash.compareTo(myHash) > 0) {
            return true;
        }

        Log.d(TAG, "OUT areYouMine");

        // You don't deserve me get out.
        return false;
    }


    private String genHash(String input) throws NoSuchAlgorithmException {

        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

}
