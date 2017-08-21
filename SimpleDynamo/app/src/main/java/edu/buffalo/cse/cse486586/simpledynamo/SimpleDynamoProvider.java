package edu.buffalo.cse.cse486586.simpledynamo;


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import android.app.Activity;
import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

@SuppressWarnings("ALL")
public class SimpleDynamoProvider extends ContentProvider {

    /* Constants */
    private static final String PREFIX = "sunny | ";
    private static final String TAG = PREFIX + SimpleDynamoProvider.class.getSimpleName() + " | [DP] ";
    private static final String S_TAG = PREFIX + ServerTask.class.getSimpleName() + " | [SP] ";
    private static final String C_TAG = PREFIX + ClientTask.class.getSimpleName() + " | [CP] ";

    /* AVD specific variables */
    private static String myPort = null;
    private static String myHash = null;
    private static Uri uri = null;

    private static ContentResolver contentResolver = null;
    private static Context context = null;

    /* Ring/Node Data Structures */
    private static String[] nodes = new String[]{"5554", "5556", "5558", "5560", "5562"};
    private static List<String> ring = new ArrayList<String>();
    private static Map<String, String> reverseLookup = new HashMap<String, String>();
    private static String[] successors = new String[]{};

    public static Semaphore replicaAckSem = new Semaphore(0);
    public static Semaphore globalQuerySem = new Semaphore(0);

    public static Lock queryLock = new ReentrantLock();
    public static Condition queryProceeedCond = queryLock.newCondition();


    public static Lock backupQueryLock = new ReentrantLock();
    public static Condition backupQueryProceeedCond = backupQueryLock.newCondition();

    public static String replicaKey = null;
    public static String queryKey = null;
    public static String queryResponse = null;
    public static String globalQueryResponse = null;
    public static String replicationResponse = null;

    public static Map<String, String> replicationStatusMap = new ConcurrentHashMap<String, String>();

    /* Backup data structures */
    public static Map<String, Map<String, String>> backupMap = new HashMap<String, Map<String, String>>();
    public static String failedNode = null;

    public static String deleteFailedNode = null;
    public static Set<String> deleteFailures = new HashSet<String>();


    @Override
    public boolean onCreate() {

        initDb();

        context = getContext();

        GlobalRepo.setContentResolver(getContext().getContentResolver());

        setPort();
        setMyHash();

        myPort = GlobalRepo.getMyPort();
        myHash = GlobalRepo.getMyHash();
        contentResolver = getContext().getContentResolver();

        buildRing();

        computeSuccessors();

        createServerSocket();

        // if this node was down and up then we have to reinitialize the state.
        initReplicationStatus();


        return false;
    }

    private void initReplicationStatus() {

        String extractQuery = "SELECT * FROM " + KeyValueContract.KeyValueEntry.TABLE_NAME;

        SQLiteOpenHelper helper = DbHelper.getInstance(getContext());
        SQLiteDatabase readableDatabase = helper.getReadableDatabase();
        Cursor cursor = readableDatabase.rawQuery(extractQuery, null);

        cursor.moveToFirst();

        int keyIndex = cursor.getColumnIndex(KeyValueContract.KeyValueEntry.KEY);
        int valueIndex = cursor.getColumnIndex(KeyValueContract.KeyValueEntry.VALUE);

        while (!cursor.isAfterLast()) {

            String rowKey = cursor.getString(keyIndex);
            String value = cursor.getString(valueIndex);
            replicationStatusMap.put(rowKey, Constants.REPLICATION_COMPLETE);

            cursor.moveToNext();
        }

        cursor.close();
    }

    /**
     *
     */
    private void initDb() {

        Log.d(Constants.LOG_PREFIX, "IN initDb");

        // Delete the current database
        //getContext().deleteDatabase(KeyValueContract.KeyValueEntry.DATABASE_NAME);
        SQLiteDatabase sqLiteDatabase = getContext().openOrCreateDatabase(KeyValueContract.KeyValueEntry.DATABASE_NAME,
                Activity.MODE_PRIVATE, null);
        sqLiteDatabase.close();

        Log.d(Constants.LOG_PREFIX, "OUT initDb");
    }

    /**
     *
     */
    private static void giveMeFuel(String destinationNode) {

        Log.d(TAG, "IN giveMeFuel");

        Log.d(TAG, "SENDING FUEL request to : " + destinationNode);

        String fuelMessage = buildMessage(new String[]{Constants.M_GIVEME_FUEL, myPort});
        ClientTask.executeSynchronousWithNoResponse(destinationNode, fuelMessage);


        Log.d(TAG, "SENT FUEL request to : " + destinationNode);

        Log.d(TAG, "OUT giveMeFuel");
    }


    @Override
    public Uri insert(Uri uri, ContentValues values) {

        Log.d(TAG, "IN insert");

        if (values.size() > 0) {

            String key = (String) values.get(KeyValueContract.KeyValueEntry.KEY);
            String value = (String) values.get(KeyValueContract.KeyValueEntry.VALUE);

            Log.d(TAG, "IN insert | Inserting key : " + key + " . Value : " + value);

            String partition = findPartition(key);

            if (myPort.equals(partition)) {

                replicationStatusMap.put(key, Constants.REPLICATION_INCOMPLETE);

                String source = (String) values.get(KeyValueContract.KeyValueEntry.SOURCE);
                values.remove(KeyValueContract.KeyValueEntry.SOURCE);

                localInsert(values, key, value);

                String nextNode = successors[0];
                String endNode = successors[1];

                if (source == null) {
                    source = myPort;
                }

                String replicaMessage = buildMessage(new String[]{Constants.M_REPLICA, key, value, endNode,
                        source});

                Log.d(TAG, "Forwarding first replication message : " + replicaMessage + ". To : " + nextNode);

                boolean error = false;

                try {

                    if (failedNode == null) {

                        ClientTask.pingExecuteSynchronousWithNoResponse(nextNode, replicaMessage);

                    } else if (failedNode.equals(nextNode)) {

                        error = true;

                    } else {

                        ClientTask.pingExecuteSynchronousWithNoResponse(nextNode, replicaMessage);
                    }
                } catch (RuntimeException e) {
                    error = true;
                    failedNode = nextNode;
                }

                if (error) {

                    String successorOfFailureNode = getSuccessor(nextNode);

                    // find the successor for a given node.
                    Log.e(TAG, "Error when replicating to node : " + nextNode + ". Forwarding replication request to : "
                            + successorOfFailureNode);

                    String replicationFailureMessage = buildMessage(new String[]{Constants.M_REPLICA_FAILURE, key,
                            value, successorOfFailureNode, source, Constants.YES_STORE});

                    Log.e(TAG, "SENDING replication mesage on failure to : " + successorOfFailureNode + ". " +
                            "Message : " +
                            "" + replicationFailureMessage);

                    ClientTask.executeSynchronousWithNoResponse(successorOfFailureNode, replicationFailureMessage);

                    Log.e(TAG, "SENT replication mesage on failure to : " + successorOfFailureNode + ". Message :" +
                            "" + replicationFailureMessage);
                }

            } else {

                // Not my partition, forward the message to the right node.

                String insertMessage = buildMessage(new String[]{Constants.M_INSERT, key, value, myPort});

                Log.d(TAG, "Forwarding key to node : " + partition + ". Message : " + insertMessage);

                boolean error = false;
                try {
                    if (failedNode == null) {

                        ClientTask.executeSynchronousWithResponse(partition, insertMessage);

                    } else if (failedNode.equals(partition)) {

                        error = true;

                    } else {
                        ClientTask.executeSynchronousWithResponse(partition, insertMessage);
                    }

                } catch (RuntimeException e) {
                    error = true;
                    failedNode = partition;
                }

                if (error) {

                    String successorOfFailureNode = getSuccessor(partition);

                    Log.e(TAG, "Error when contacting the right partition : " + partition + ". Forwarding " +
                            "insert request to : " + successorOfFailureNode);


                    String insertFailureMessage = buildMessage(new String[]{Constants.M_INSERT_FAILURE, key,
                            value, myPort});

                    Log.e(TAG, "SENDING insert mesage on failure to : " + successorOfFailureNode + ". " +
                            "Message : " + insertFailureMessage);

                    ClientTask.executeSynchronousWithNoResponse(successorOfFailureNode, insertFailureMessage);

                    Log.e(TAG, "SENT insert mesage on failure to : " + successorOfFailureNode + ". Message :" +
                            "" + insertFailureMessage);

                }

                Log.d(TAG, "Waiting for Replication ACK");
                // FIXME: Handle this logic. Both the command line and the server could be accessing this at the same
                // time.. Think about that scenario.. Reason and fix this issue.

                try {
                    replicaKey = key;
                    replicaAckSem.acquire();
                    replicaKey = null;
                } catch (InterruptedException e) {
                    Log.d(TAG, "Error occurred when acquiring Replication ACK semaphore");
                    e.printStackTrace();
                }
                Log.d(TAG, "Recieved Replication ACK");
            }
        }

        Log.d(TAG, "OUT insert");

        return uri;
    }


    /**
     * @param values
     * @param key
     * @param value
     */
    private static synchronized void localInsert(ContentValues values, String key,
                                                 String value) {

        // FIXME : This should be synchrnoized. Think about it.

        Log.d(TAG, "IN localInsert");

        SQLiteDatabase sqLiteDatabase = DbHelper.getInstance(context).getWritableDatabase();

        String findQuery = QueryBuilder.buildFind(KeyValueContract.KeyValueEntry.TABLE_NAME, key);
        Cursor cursor = sqLiteDatabase.rawQuery(findQuery, null);

        values.remove(KeyValueContract.KeyValueEntry.REP);
        values.remove(KeyValueContract.KeyValueEntry.SOURCE);

        if (cursor.moveToFirst()) {

            String updateStatement = QueryBuilder.buildUpdate(KeyValueContract.KeyValueEntry.TABLE_NAME,
                    key, value);
            Log.d(TAG, updateStatement);

            ContentValues contentValues = new ContentValues();
            contentValues.put(KeyValueContract.KeyValueEntry.KEY, key);
            contentValues.put(KeyValueContract.KeyValueEntry.VALUE, value);

            String whereClause = "key=" + "'" + key + "'";
            sqLiteDatabase.update(KeyValueContract.KeyValueEntry.TABLE_NAME, contentValues, whereClause,
                    null);

            // sqLiteDatabase.rawQuery(updateStatement, null);

        } else {

            Log.d(TAG, "Inserting key : " + key + " , value : " + value);
            sqLiteDatabase.insert(KeyValueContract.KeyValueEntry.TABLE_NAME, null, values);
        }

        cursor.close();

        Log.d(TAG, "OUT localInsert");
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

            // Query all the nodes in the system
            cursor = globalQuery(key);

        } else {

            // single query
            Log.d(TAG, "Doing single query for key : " + key);

            // this is regular query from cmd.. find the correct node and ask the read node.

            String partition = findPartition(key);
            String readNode = getReadNode(partition);

            if (myPort.equals(readNode)) {

                /*
                References : http://www.math.uni-hamburg.de/doc/java/tutorial/essential/threads/explicitlocks.html
                 */
                queryLock.lock();
                try {

                    while (replicationStatusMap.get(key) == null
                            || replicationStatusMap.get(key).equals(Constants.REPLICATION_INCOMPLETE)) {

                        Log.d(TAG, "Acquiring lock in query");
                        try {
                            queryProceeedCond.await();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                } finally {
                    queryLock.unlock();
                }

                Log.d(TAG, "Releasing lock in query. Replication is complete for key : " + key);
                Log.d(TAG, "Replication is complete for key : " + key);

                SQLiteOpenHelper helper = DbHelper.getInstance(context);
                SQLiteDatabase readableDatabase = helper.getReadableDatabase();
                cursor = CommonServerProcessingMethods.localQuery(readableDatabase, key);

            } else {

                // send it to the right node
                String queryMessage = Constants.M_QUERY + Constants.DELIMITER + key + Constants.DELIMITER +
                        myPort;

                Log.d(TAG, "Sending query message to read node : " + readNode + ". Message : " + queryMessage);

                String response = null;

                boolean error = false;
                try {
                    if (failedNode == null) {

                        response = ClientTask.executeSynchronousWithResponse(readNode, queryMessage);

                    } else if (failedNode.equals(readNode)) {

                        error = true;

                    } else {
                        response = ClientTask.executeSynchronousWithResponse(readNode, queryMessage);
                    }
                } catch (RuntimeException e) {
                    error = true;
                    failedNode = readNode;
                }

                if (error) {

                    String predecessorOfFailureNode = getPredecessor(readNode);

                    // find the successor for a given node.
                    Log.e(TAG, "Error when querying node : " + readNode + ". Forwarding query request to : "
                            + predecessorOfFailureNode);

                    String queryFailureMessage = buildMessage(new String[]{Constants.M_QUERY_FAILURE, key, myPort});

                    Log.e(TAG, "SENDING query mesage on failure to : " + predecessorOfFailureNode + ". " +
                            "Message : " + queryFailureMessage);

                    response = ClientTask.executeSynchronousWithResponse(predecessorOfFailureNode,
                            queryFailureMessage);

                    Log.e(TAG, "SENT query mesage on failure to : " + predecessorOfFailureNode + ". Message :" +
                            "" + queryFailureMessage);
                }

                String[] split = response.split(Constants.ESC_DELIMITER);
                String value = split[2];

                Log.d(TAG, "Query result for Key : " + key + " , Value : " + value);

                /* Reference: http://chariotsolutions.com/blog/post/android-advanced-cursors/ */

                MatrixCursor matrixCursor = new MatrixCursor(new String[]{KeyValueContract.KeyValueEntry.KEY,
                        KeyValueContract.KeyValueEntry.VALUE});

                if (!value.equals(Constants.NOROW)) {
                    matrixCursor.addRow(new String[]{key, value});
                }

                cursor = matrixCursor;
            }
        }

        Log.d(TAG, "OUT query");
        return cursor;
    }

    /**
     * @param key
     * @return
     */
    private Cursor globalQuery(String key) {

        Cursor cursor;
        String queryMessage = Constants.M_GLOBAL_QUERY + Constants.DELIMITER + key + Constants.DELIMITER + myPort;

        MatrixCursor matrixCursor = new MatrixCursor(new String[]{KeyValueContract.KeyValueEntry.KEY,
                KeyValueContract.KeyValueEntry.VALUE});

        for (String node : nodes) {

            Log.d(TAG, "Requesting global query for node : " + node);

            String response = null;

            boolean error = false;
            try {

                if (failedNode == null) {

                    response = ClientTask.executeSynchronousWithResponse(node, queryMessage);

                } else if (failedNode.equals(node)) {

                    error = true;

                } else {

                    response = ClientTask.executeSynchronousWithResponse(node, queryMessage);
                }
            } catch (RuntimeException e) {
                error = true;
                failedNode = node;
            }

            if (error) {

                String predecessorOfFailureNode = getPredecessor(node);

                // find the successor for a given node.
                Log.e(TAG, "Error when doing global query for node : " + node + ". Forwarding global query request to" +
                        " : " + predecessorOfFailureNode);

                String queryFailureMessage = buildMessage(new String[]{Constants.M_GLOBAL_QUERY_FAILURE, key, myPort});

                Log.e(TAG, "SENDING global query mesage on failure to : " + predecessorOfFailureNode + ". " +
                        "Message : " + queryFailureMessage);

                response = ClientTask.executeSynchronousWithResponse(predecessorOfFailureNode,
                        queryFailureMessage);

                Log.e(TAG, "SENT global query mesage on failure to : " + predecessorOfFailureNode + ". Message :" +
                        "" + queryFailureMessage);
            }

            if (response != null) {

                String[] split = response.split(Constants.ESC_DELIMITER);

                for (int i = 2; i < split.length; i = i + 2) {
                    matrixCursor.addRow(new String[]{split[i], split[i + 1]});
                }
            }
        }
        cursor = matrixCursor;

        return cursor;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        /* References : http://developer.android.com/training/basics/data-storage/databases.html#DeleteDbRow
        * http://stackoverflow.com/questions/7510219/deleting-row-in-sqlite-in-android
        * http://stackoverflow.com/questions/3094444/delete-all-rows-from-a-table-throws-nullpointer
        */

        Log.d(TAG, "IN delete");

        int delete = -1;
        String key = selection;

        if (key.equals(Constants.LOCAL)) {

            Log.d(TAG, "delete | LOCAL");

            delete = deleteAll();

            return delete;
        }

        if (key.equals(Constants.GLOBAL)) {

            Log.d(TAG, "delete | GLOBAL");

            String globalDeleteMessage = Constants.M_GLOBAL_DELETE;

            Log.d(TAG, "Sending Global Delete to all the nodes : " + globalDeleteMessage);

            for (String node : nodes) {

                if (!myPort.equals(node)) {
                    //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, node, globalDeleteMessage);
                    ClientTask.executeSynchronousWithNoResponse(node, globalDeleteMessage);
                }
            }

            delete = deleteAll();
            return delete;
        }


        if (selectionArgs != null && selectionArgs.length == 1 && selectionArgs[0].equals(Constants
                .M_DELETE_REPLICA)) {

            Log.d(TAG, "delete | DELETE REPLICA");

            delete = delete(key);

            return delete;
        }

        Log.d(TAG, "delete | SINGLE ROW");

        String partition = findPartition(key);

        if (myPort.equals(partition)) {

            delete = delete(key);

            Log.d(TAG, "Deleting the replicas on : " + successors[0] + ", " + successors[1]);

            String deleteReplicaMessage = Constants.M_DELETE_REPLICA + Constants.DELIMITER + key + Constants
                    .DELIMITER +
                    myPort;

            for (String node : successors) {

                Log.d(TAG, "Sending delete replica message to node : " + node);

                //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, node, deleteReplicaMessage);
                ClientTask.executeSynchronousWithNoResponse(node, deleteReplicaMessage);

                /*
                String[] split = response.split(Constants.ESC_DELIMITER);
                String rows = split[1];
                Log.d(TAG, "Node : " + node + " deleted " + rows + " rows");
                */
            }

        } else {

            // send it to the right node
            String deleteMessage = Constants.M_DELETE + Constants.DELIMITER + key + Constants.DELIMITER + myPort;

            Log.d(TAG, "Sending delete message to node : " + partition);

            //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, partition, deleteMessage);
            ClientTask.executeSynchronousWithNoResponse(partition, deleteMessage);
        }

        Log.d(TAG, "OUT delete");

        return delete;
    }

    public static String decrementString(String number) {

        int num = Integer.parseInt(number) - 1;
        return num + "";

    }

    /**
     * @param key
     * @param howManyTimes
     */
    private static void deleteReplica(String key, String howManyTimes) {


        String nextNode = successors[0];
        Log.d(TAG, "Sending delete replica message to node : " + nextNode);

        String deleteReplicaMessage = buildMessage(new String[]{Constants.M_DELETE_REPLICA, key, howManyTimes});

        boolean error = false;
        try {

            if (deleteFailedNode == null) {

                ClientTask.executeSynchronousWithResponse(nextNode, deleteReplicaMessage);

            } else if (deleteFailedNode.equals(nextNode)) {

                error = true;

            } else {

                ClientTask.executeSynchronousWithResponse(nextNode, deleteReplicaMessage);
            }

        } catch (RuntimeException e) {
            error = true;
            deleteFailedNode = nextNode;
        }


        if (error) {

            String updatedHowManyTimes = decrementString(howManyTimes);

            if (updatedHowManyTimes.equals("0")) {

                Log.d(TAG, "Failed to reach the last node. Hence done with delete replication for key : " + key);

                Log.d(S_TAG, "Backing up the failure : " + key);

                deleteFailures.add(key);

                Log.d(S_TAG, "Done Backing up the failure : " + key);

            } else {

                String successorOfFailureNode = getSuccessor(nextNode);

                Log.e(TAG, "Error when contacting the right partition for delete : " + nextNode + ". Forwarding " +
                        "delete request to : " + successorOfFailureNode);


                String deleteReplicaFailureMessage = buildMessage(new String[]{Constants.M_DELETE_REPLICA_FAILURE, key,
                        updatedHowManyTimes});

                Log.e(TAG, "SENDING delete replica mesage on failure to : " + successorOfFailureNode + ". " +
                        "Message : " + deleteReplicaFailureMessage);

                ClientTask.executeSynchronousWithNoResponse(successorOfFailureNode, deleteReplicaFailureMessage);

                Log.e(TAG, "SENT delete replica mesage on failure to : " + successorOfFailureNode + ". Message :" +
                        "" + deleteReplicaFailureMessage);
            }
        }
    }

    /**
     * @param key
     * @return
     */
    private static int delete(String key) {

        int delete;
        SQLiteOpenHelper helper = DbHelper.getInstance(context);
        SQLiteDatabase writableDatabase = helper.getWritableDatabase();

        delete = writableDatabase.delete(KeyValueContract.KeyValueEntry.TABLE_NAME,
                KeyValueContract.KeyValueEntry.KEY + "='" + key + "'", null);

        return delete;
    }

    /**
     * @return
     */
    private int deleteAll() {

        SQLiteOpenHelper helper = DbHelper.getInstance(getContext());
        SQLiteDatabase writableDatabase = helper.getWritableDatabase();
        int delete = writableDatabase.delete(KeyValueContract.KeyValueEntry.TABLE_NAME, null, null);
        writableDatabase.close();

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
     * @param key
     * @return
     */
    private String findPartition(String key) {

        Log.d(TAG, "IN findPartition");

        String hashedKey = Utils.genHash(key);

        int len = ring.size();
        LinkedList<String> hashedNodes = new LinkedList<String>(ring);
        hashedNodes.add(hashedKey);
        Collections.sort(hashedNodes);

        int pos = -1;
        Iterator<String> iterator = hashedNodes.iterator();

        while (iterator.hasNext()) {

            pos++;
            if (hashedKey.equals(iterator.next())) {
                break;
            }
        }

        int partitionIndex = Utils.mod(pos, len);
        String partition = ring.get(partitionIndex);


        Log.d(TAG, "OUT findPartition");

        return reverseLookup.get(partition);
    }


    /**
     *
     */
    private void buildRing() {

        Log.d(TAG, "In buildRing");

        for (String node : nodes) {

            String hashedNode = Utils.genHash(node);
            ring.add(hashedNode);
            reverseLookup.put(hashedNode, node);
        }
        // sort them and keep it ready.
        Collections.sort(ring);

        GlobalRepo.ring = ring;
        GlobalRepo.reverseLookup = reverseLookup;

        Log.d(TAG, "Out buildRing");
    }


    /**
     *
     */
    private void computeSuccessors() {

        int len = ring.size();
        int myIndex = ring.indexOf(myHash);
        int successorOne = Utils.mod(myIndex + 1, len);
        int successorTwo = Utils.mod(myIndex + 2, len);

        successors = new String[2];
        successors[0] = reverseLookup.get(ring.get(successorOne));
        successors[1] = reverseLookup.get(ring.get(successorTwo));


        GlobalRepo.successors = successors;
    }


    /**
     * @param input
     * @return
     */
    private static String buildMessage(String[] input) {

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < input.length; i++) {

            sb.append(input[i]);

            if (i != input.length - 1) {
                sb.append(Constants.DELIMITER);
            }
        }

        return sb.toString();
    }


    /**
     *
     */
    private void setPort() {

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        String myPort = String.valueOf((Integer.parseInt(portStr)));

        GlobalRepo.setMyPort(myPort);
    }


    private void setMyHash() {

        String myHash = Utils.genHash(GlobalRepo.getMyPort());
        GlobalRepo.setMyHash(myHash);
    }

    /**
     * @return
     */
    private void createServerSocket() {

        try {

            ServerSocket serverSocket = new ServerSocket(Constants.SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

        } catch (Exception e) {

            Log.e(TAG, "Error when creating ServerSocket : " + e.getMessage());
        }
    }

    /**
     * @param partition
     * @return
     */
    private String getReadNode(String partition) {

        Log.d(TAG, "IN getReadNode");

        int len = ring.size();
        String partitionHash = Utils.genHash(partition);
        int partitionIndex = ring.indexOf(partitionHash);
        int successorOne = Utils.mod(partitionIndex + 1, len);
        int successorTwo = Utils.mod(partitionIndex + 2, len);

        String[] succ = new String[2];
        succ[0] = reverseLookup.get(ring.get(successorOne));
        succ[1] = reverseLookup.get(ring.get(successorTwo));

        Log.d(TAG, "OUT getReadNode");

        return succ[1];
    }

    /**
     * @param currentNode
     * @return
     */
    private static String getSuccessor(String currentNode) {

        Log.d(TAG, "IN getSuccessor");

        int len = ring.size();
        String partitionHash = Utils.genHash(currentNode);
        int partitionIndex = ring.indexOf(partitionHash);
        int successorOne = Utils.mod(partitionIndex + 1, len);

        String successor = reverseLookup.get(ring.get(successorOne));

        Log.d(TAG, "OUT getSuccessor");

        return successor;

    }


    /**
     * @param node
     * @return
     */
    private static String getPredecessor(String node) {

        Log.d(TAG, "IN getPredecessor");

        int len = ring.size();
        String partitionHash = Utils.genHash(node);
        int partitionIndex = ring.indexOf(partitionHash);
        int predecessorOne = Utils.mod(partitionIndex - 1, len);

        String predecessor = reverseLookup.get(ring.get(predecessorOne));

        Log.d(TAG, "OUT getPredecessor");

        return predecessor;

    }

    public static class ServerTask extends AsyncTask<ServerSocket, Void, Void> {


        private static String myPort = null;
        private static ContentResolver contentResolver = null;

        /**
         * @param sockets
         * @return
         */
        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            // set the content resolver
            contentResolver = GlobalRepo.getContentResolver();
            myPort = GlobalRepo.getMyPort();
            buildURI();

            String successor = getSuccessor(myPort);
            String predecessor = getPredecessor(myPort);

            // give me fuel
            giveMeFuel(successor);
            giveMeFuel(predecessor);

            for (String node : nodes) {

                if (node.equals(successor) || node.equals(myPort) || node.equals(predecessor)) {
                    continue;
                }

                Log.d(TAG, "Sending RESET request to : " + node);

                String resetMessage = buildMessage(new String[]{Constants.M_RESET, myPort});
                ClientTask.executeSynchronousWithNoResponse(node, resetMessage);

                Log.d(TAG, "SENT RESET request to : " + node);
            }

            ServerSocket serverSocket = sockets[0];

            try {

                while (true) {

                    Log.d(S_TAG, "Server Waiting.....");

                    Socket clientSocket = serverSocket.accept();
                    spawn(clientSocket);

                    Log.d(S_TAG, "Server spawned.....");
                }
            } catch (IOException e) {
                e.printStackTrace();
                Log.e(S_TAG, "Error in ServerTaskUsed : " + e.getMessage());
            }

            return null;
        }


        /**
         * @param clientSocket
         * @throws IOException
         */
        private void spawn(Socket clientSocket) throws IOException {

            Thread serverThread = new Thread(new ServerRequestHandler(clientSocket));
            serverThread.start();
        }

        /**
         *
         */
        private void buildURI() {

            Uri.Builder builder = new Uri.Builder();
            builder.scheme(Constants.CONTENT);
            builder.authority(Constants.AUTHORITY);
            uri = builder.build();

            GlobalRepo.setUri(uri);
        }

    }


    public static class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            try {

                String destination = msgs[0];
                String message = msgs[1];


                String actualDest = GlobalRepo.getPortNumber(destination);
                Socket socket = new Socket(InetAddress.getByAddress
                        (new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(actualDest));


                Log.d(C_TAG, "Sending to : " + destination + ". Message : " + message);

                DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                outputStream.writeUTF(message);
                outputStream.flush();

                outputStream.close();
                socket.close();


            } catch (Exception e) {

                e.printStackTrace();
                Log.e(C_TAG, "Error in ClientTask : " + e.getMessage());
                throw new RuntimeException(e);
            }

            return null;
        }


        public static void executeSynchronousWithNoResponse(String destination, String message) {


            Log.d(C_TAG, "IN executeSynchronousWithNoResponse");

            try {

                String actualDest = GlobalRepo.getPortNumber(destination);

                Socket socket = new Socket(InetAddress.getByAddress
                        (new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(actualDest));

                Log.d(C_TAG, "Sending to : " + destination + ". Message : " + message);

                DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                outputStream.writeUTF(message);

                outputStream.flush();
                outputStream.close();
                socket.close();

            } catch (Exception e) {

                e.printStackTrace();
                Log.e(C_TAG, "Error in ClientTask : " + e.getMessage());
                throw new RuntimeException(e);
            }

            Log.d(C_TAG, "OUT executeSynchronousWithNoResponse");
        }


        public static String executeSynchronousWithResponse(String destination, String message) {

            Log.d(C_TAG, "IN executeSynchronousWithResponse");

            String response = null;

            try {


                String actualDest = GlobalRepo.getPortNumber(destination);

                Socket socket = new Socket(InetAddress.getByAddress
                        (new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(actualDest));
                // socket.setSoTimeout(Constants.TIME_OUT);

                Log.d(C_TAG, "Sending to : " + destination + ". Message : " + message);

                DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                DataInputStream inputStream = new DataInputStream(socket.getInputStream());

                outputStream.writeUTF(message);
                outputStream.flush();

                Log.d(C_TAG, "Waiting for : " + destination + " to return data");

                response = inputStream.readUTF();

                Log.d(C_TAG, "executeSynchronousWithResponse | Received response : " + response);


                outputStream.close();
                inputStream.close();
                socket.close();

            } catch (Exception e) {

                e.printStackTrace();
                Log.e(C_TAG, "Error in ClientTask : " + e.getMessage());
                throw new RuntimeException(e);
            }

            Log.d(C_TAG, "OUT executeSynchronousWithResponse");
            return response;
        }


        public static void pingExecuteSynchronousWithNoResponse(String destination, String message) {


            Log.d(C_TAG, "IN pingExecuteSynchronousWithNoResponse");

            try {

                String actualDest = GlobalRepo.getPortNumber(destination);

                Socket socket = new Socket(InetAddress.getByAddress
                        (new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(actualDest));
                //socket.setSoTimeout(Constants.TIME_OUT);

                Log.d(C_TAG, "Sending PING to : " + destination + ". Message : " + Constants.PING);

                DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                DataInputStream inputStream = new DataInputStream(socket.getInputStream());

                outputStream.writeUTF(Constants.PING);

                String ack = inputStream.readUTF();

                Log.d(C_TAG, "Recieved ACK from : " + destination + ". Response : " + ack);

                inputStream.close();
                outputStream.close();
                socket.close();

            } catch (Exception e) {

                e.printStackTrace();
                Log.e(C_TAG, "Error in pingExecuteSynchronousWithNoResponse : " + e.getMessage());
                throw new RuntimeException(e);
            }


            try {

                String actualDest = GlobalRepo.getPortNumber(destination);

                Socket socket = new Socket(InetAddress.getByAddress
                        (new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(actualDest));

                Log.d(C_TAG, "Sending to : " + destination + ". Message : " + message);

                DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                outputStream.writeUTF(message);

                outputStream.flush();
                outputStream.close();
                socket.close();

            } catch (Exception e) {

                e.printStackTrace();
                Log.e(C_TAG, "Error in ClientTask : " + e.getMessage());
                throw new RuntimeException(e);
            }

            Log.d(C_TAG, "OUT pingExecuteSynchronousWithNoResponse");
        }
    }

    public static class ServerRequestHandler implements Runnable {


        Socket clientSocket = null;

        public ServerRequestHandler(Socket clientSocket) {

            this.clientSocket = clientSocket;
        }


        @Override
        public void run() {

            try {

                //String response = "ACK";
                String response = null;
                DataInputStream inputStream = new DataInputStream(clientSocket.getInputStream());
                DataOutputStream outputStream = new DataOutputStream(clientSocket.getOutputStream());

                String message = inputStream.readUTF();

                Log.d(S_TAG, ">>>>>>> Received message : " + message);

                if (null == message) {

                    Log.e(S_TAG, "Message was null");

                } else if (message.startsWith(Constants.PING)) {

                    response = Constants.ACK;

                } else if (message.startsWith(Constants.M_RESET)) {

                    failedNode = null;
                    deleteFailedNode = null;
                    deleteFailures = new HashSet<String>();
                    setReset(true);

                } else if (message.startsWith(Constants.M_DUMP)) {

                    CommonServerProcessingMethods.doDump(message);

                } else if (message.startsWith(Constants.M_DELETE_DUMP)) {

                    CommonServerProcessingMethods.doDeleteDump(message);

                } else if (message.startsWith(Constants.M_GIVEME_FUEL)) {

                    CommonServerProcessingMethods.doGiveMeFuel(message);
                    setReset(true);

                } else if (message.startsWith(Constants.M_GIVEME_FUEL_RESPONSE)) {

                    CommonServerProcessingMethods.doGiveMeFuelResponse(message);

                } else if (message.startsWith(Constants.M_INSERT)) {

                    CommonServerProcessingMethods.doInsert(message);
                    response = Constants.M_INSERT_SUCCESS;

                } else if (message.startsWith(Constants.M_INSERT_FAILURE)) {

                    CommonServerProcessingMethods.doInsertFailure(message);

                } else if (message.startsWith(Constants.M_REPLICA)) {

                    CommonServerProcessingMethods.doReplica(message);

                } else if (message.startsWith(Constants.M_REPLICA_FAILURE)) {

                    CommonServerProcessingMethods.doReplicaFailure(message);

                } else if (message.startsWith(Constants.M_REPLICA_RESPONSE)) {

                    CommonServerProcessingMethods.doReplicaResponse(message);

                } else if (message.startsWith(Constants.M_QUERY)) {

                    // single query
                    response = CommonServerProcessingMethods.doQuery(message);

                } else if (message.startsWith(Constants.M_QUERY_FAILURE)) {

                    // query failure
                    response = CommonServerProcessingMethods.doQueryFailure(message);

                } else if (message.startsWith(Constants.M_GLOBAL_QUERY)) {

                    // global query
                    response = CommonServerProcessingMethods.doGlobalQuery(message);

                } else if (message.startsWith(Constants.M_GLOBAL_QUERY_FAILURE)) {

                    // global query
                    //response = CommonServerProcessingMethods.doGlobalQueryFailure(message);
                    response = CommonServerProcessingMethods.doGlobalQuery(message);

                } else if (message.startsWith(Constants.M_DELETE)) {

                    // single query
                    response = CommonServerProcessingMethods.doDelete(message);

                } else if (message.startsWith(Constants.M_DELETE_FAILURE)) {

                    // single query
                    CommonServerProcessingMethods.doDeleteFailure(message);

                } else if (message.startsWith(Constants.M_DELETE_REPLICA)) {

                    // single query
                    response = CommonServerProcessingMethods.doDeleteReplica(message);

                } else if (message.startsWith(Constants.M_DELETE_REPLICA_FAILURE)) {

                    // single query
                    CommonServerProcessingMethods.doDeleteReplicaFailure(message);

                } else if (message.startsWith(Constants.M_GLOBAL_DELETE)) {

                    // global query delete
                    CommonServerProcessingMethods.doGlobalDelete(message);
                    response = "GLOBAL_DELETE_DONE";

                } else if (message.startsWith(Constants.M_GLOBAL_DELETE_FAILURE)) {

                    // global query delete
                    CommonServerProcessingMethods.doGlobalDeleteFailure(message);
                }

                if (response != null) {

                    Log.d(S_TAG, "Sending response : " + response);

                    outputStream.writeUTF(response);
                    outputStream.flush();

                    Log.d(S_TAG, "Sent response : " + response);
                }

                inputStream.close();
                outputStream.close();
                clientSocket.close();

                Log.d(S_TAG, ">>>>>>> End Processing received message : " + message);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    /**
     *
     */
    public static class CommonServerProcessingMethods {


        public static void doGiveMeFuel(String message) {

            failedNode = null;

            Log.d(S_TAG, "IN doGiveMeFuel");

            Log.d(S_TAG, "doGiveMeFuel | FUEL message : " + message);

            String[] split = message.split(Constants.ESC_DELIMITER);
            String source = split[1];

            String response = "";
            Map<String, String> keyValueMap = backupMap.get(source);

            if (keyValueMap == null) {

                Log.e(S_TAG, "No backup found for you... Don't bother me...");
                response = "M_GIVEME_FUEL_RESPONSE" + Constants.DELIMITER + "NULL";

            } else {

                StringBuilder fuelResponse = new StringBuilder();
                fuelResponse.append(Constants.M_GIVEME_FUEL_RESPONSE);
                fuelResponse.append(Constants.DELIMITER);
                fuelResponse.append(myPort);

                for (Map.Entry<String, String> entry : keyValueMap.entrySet()) {

                    fuelResponse.append(Constants.DELIMITER);
                    fuelResponse.append(entry.getKey());
                    fuelResponse.append(Constants.DELIMITER);
                    fuelResponse.append(entry.getValue());
                }
                response = fuelResponse.toString();

                backupMap.put(source, null);
                ClientTask.executeSynchronousWithNoResponse(source, response);

            }

            if (!deleteFailures.isEmpty()) {

                StringBuilder sb = new StringBuilder();
                sb.append(Constants.M_DELETE_DUMP);
                sb.append(Constants.DELIMITER);
                sb.append(myPort);

                if (deleteFailures.contains(Constants.LOCAL)) {

                    sb.append(Constants.DELIMITER);
                    sb.append(Constants.LOCAL);

                } else {

                    Iterator<String> iterator = deleteFailures.iterator();
                    while (iterator.hasNext()) {
                        sb.append(Constants.DELIMITER);
                        sb.append(iterator.next());
                    }

                    String deleteDumpMessage = sb.toString();
                    ClientTask.executeSynchronousWithNoResponse(source, deleteDumpMessage);
                }
                deleteFailures = new HashSet<String>();
            }

            Log.d(S_TAG, "OUT doGiveMeFuel");
        }

        /**
         * @param message
         */
        public static void doGiveMeFuelResponse(String message) {

            Log.d(S_TAG, "IN doGiveMeFuelResponse");

            Log.d(S_TAG, "doGiveMeFuelResponse | FUEL message : " + message);

            if (!message.isEmpty()) {

                String[] split = message.split(Constants.ESC_DELIMITER);

                Log.d(TAG, "Saving response to DB");

                for (int i = 2; i < split.length; i = i + 2) {

                    String key = split[i];
                    String value = split[i + 1];

                    ContentValues contentValues = new ContentValues();
                    contentValues.put(KeyValueContract.KeyValueEntry.KEY, key);
                    contentValues.put(KeyValueContract.KeyValueEntry.VALUE, value);

                    localInsert(contentValues, key, value);
                    replicationStatusMap.put(key, Constants.REPLICATION_COMPLETE);
                }

                Log.d(TAG, "Done saving response to DB");
            }

            Log.d(S_TAG, "OUT doGiveMeFuelResponse");
        }

        /**
         * @param message
         */
        public static void doDump(String message) {

            Log.d(S_TAG, "IN doDump");

            Log.d(S_TAG, "doDump | DUMP message : " + message);
            String[] split = message.split(Constants.ESC_DELIMITER);
            String key = split[1];
            String value = split[2];
            String source = split[3];


            Log.d(S_TAG, "Saving to DB");

            ContentValues contentValues = new ContentValues();
            contentValues.put(KeyValueContract.KeyValueEntry.KEY, key);
            contentValues.put(KeyValueContract.KeyValueEntry.VALUE, value);
            contentValues.put(KeyValueContract.KeyValueEntry.SOURCE, source);

            localInsert(contentValues, key, value);
            replicationStatusMap.put(key, Constants.REPLICATION_COMPLETE);

            Log.d(S_TAG, "Done saving to DB");

            Log.d(S_TAG, "Signalling");
            try {

                queryLock.lock();
                queryProceeedCond.signalAll();

            } finally {
                queryLock.unlock();
            }

            Log.d(S_TAG, "Done signalling");

            Log.d(S_TAG, "OUT doDump");
        }

        /**
         * @param message
         */
        public static void doDeleteDump(String message) {

            Log.d(S_TAG, "IN doDeleteDump");

            Log.d(S_TAG, "doDeleteDump | DELETE DUMP message : " + message);

            String[] split = message.split(Constants.ESC_DELIMITER);

            if (split.length == 3) {

                Log.d(S_TAG, "Deleting all keys");
                int rows = contentResolver.delete(uri, Constants.LOCAL, null);

            } else {

                Log.d(S_TAG, "Deleting keys");

                for (int i = 2; i < split.length; i++) {

                    String key = split[i];

                    Log.d(S_TAG, "Deleting key : " + key);

                    delete(key);
                }
            }

            Log.d(S_TAG, "Done Deleting keys");

            Log.d(S_TAG, "OUT doDeleteDump");
        }


        /**
         * @param message
         */
        private static void doInsert(String message) {

            Log.d(S_TAG, "IN doInsert");

            Log.d(S_TAG, "doInsert | Insert message : " + message);

            String[] split = message.split(Constants.ESC_DELIMITER);
            String key = split[1];
            String value = split[2];
            String source = split[3];

            ContentValues contentValues = new ContentValues();
            contentValues.put(KeyValueContract.KeyValueEntry.KEY, key);
            contentValues.put(KeyValueContract.KeyValueEntry.VALUE, value);
            contentValues.put(KeyValueContract.KeyValueEntry.SOURCE, source);

            contentResolver.insert(uri, contentValues);


            Log.d(S_TAG, "OUT doInsert");
        }

        /**
         * @param message
         */
        private static void doInsertFailure(String message) {

            Log.d(S_TAG, "IN doInsertFailure");

            Log.d(S_TAG, "doInsertFailure | Insert failure message : " + message);

            String[] split = message.split(Constants.ESC_DELIMITER);
            String key = split[1];
            String value = split[2];
            String source = split[3];

            Log.d(TAG, "Saving ino local backup map");

            String failedNode = getPredecessor(myPort);
            insertIntoBackupMap(failedNode, key, value);

            if (getReset()) {
                // send this to the failed node to store;

                String dumpMessage = buildMessage(new String[]{Constants.M_DUMP, key, value, myPort});
                ClientTask.executeSynchronousWithNoResponse(failedNode, dumpMessage);
            }

            Log.d(TAG, "Saved into local backup map");

            Log.d(TAG, "Doing local insert");

            ContentValues contentValues = new ContentValues();
            contentValues.put(KeyValueContract.KeyValueEntry.KEY, key);
            contentValues.put(KeyValueContract.KeyValueEntry.VALUE, value);
            contentValues.put(KeyValueContract.KeyValueEntry.SOURCE, source);

            localInsert(contentValues, key, value);

            Log.d(TAG, "Done local insert");

            Log.d(TAG, "Forwarding replica");

            String endNode = getSuccessor(myPort);
            CommonServerProcessingMethods.replicateReplica(key, value, endNode, source);

            Log.d(TAG, "Done forwarding replica");


            Log.d(S_TAG, "OUT doInsertFailure");
        }


        /**
         * @param message
         */
        private static void doReplica(String message) {

            Log.d(S_TAG, "IN doReplica");

            Log.d(S_TAG, "doReplica | Replicating message : " + message);

            String[] split = message.split(Constants.ESC_DELIMITER);
            String key = split[1];
            String value = split[2];
            String endNode = split[3];
            String source = split[4];

            ContentValues contentValues = new ContentValues();
            contentValues.put(KeyValueContract.KeyValueEntry.KEY, key);
            contentValues.put(KeyValueContract.KeyValueEntry.VALUE, value);
            contentValues.put(KeyValueContract.KeyValueEntry.REP, endNode);
            contentValues.put(KeyValueContract.KeyValueEntry.SOURCE, source);

            CommonServerProcessingMethods.replicate(contentValues);

            replicationStatusMap.put(key, Constants.REPLICATION_COMPLETE);

            Log.d(S_TAG, "OUT doReplica");

        }

        /**
         * @param message
         */
        private static void doReplicaFailure(String message) {

            Log.d(S_TAG, "IN doReplicaFailure");

            Log.d(S_TAG, "doReplicaFailure | Replicating message : " + message);

            String[] split = message.split(Constants.ESC_DELIMITER);
            String key = split[1];
            String value = split[2];
            String endNode = split[3];
            String source = split[4];
            String store = split[5];

            String failedNode = getPredecessor(myPort);
            insertIntoBackupMap(failedNode, key, value);

            if (getReset()) {
                // send this to the failed node to store;

                String dumpMessage = buildMessage(new String[]{Constants.M_DUMP, key, value, myPort});
                ClientTask.executeSynchronousWithNoResponse(failedNode, dumpMessage);
            }

            if (store.equals(Constants.YES_STORE)) {

                ContentValues contentValues = new ContentValues();
                contentValues.put(KeyValueContract.KeyValueEntry.KEY, key);
                contentValues.put(KeyValueContract.KeyValueEntry.VALUE, value);
                contentValues.put(KeyValueContract.KeyValueEntry.REP, endNode);
                contentValues.put(KeyValueContract.KeyValueEntry.SOURCE, source);

                localInsert(contentValues, key, value);

            } else {

                // no need to store in the database. just send an ack back to release the waiting node.
            }

            // This will send the acknowledgement back
            CommonServerProcessingMethods.replicateReplica(key, value, endNode, source);

            Log.d(S_TAG, "OUT doReplicaFailure");

        }

        /**
         * @param failedNode
         * @param key
         * @param value
         */
        private static void insertIntoBackupMap(String failedNode, String key, String value) {

            if (backupMap.get(failedNode) == null) {

                Map<String, String> keyValueMap = new HashMap<String, String>();
                keyValueMap.put(key, value);
                backupMap.put(failedNode, keyValueMap);

            } else {

                backupMap.get(failedNode).put(key, value);
            }
        }

        /**
         * @param message
         */
        private static void doReplicaResponse(String message) {

            Log.d(S_TAG, "IN doReplicaResponse");

            Log.d(S_TAG, "doReplicaResponse | Replication ACK : " + message);

            String[] split = message.split(Constants.ESC_DELIMITER);
            String key = split[1];
            String value = split[2];

            // Release the key only when receive acknowledgement for this key.
            // FIXME: Could be a problem. Think of all cases. Or eliminate sending the replica response.
            // This could be a problem when the replication machine fails in the middle of the execution
            //if (replicaKey != null && replicaKey.equals(key)) {
            replicaAckSem.release();
            //}

            replicationStatusMap.put(key, Constants.REPLICATION_COMPLETE);

            Log.d(S_TAG, "OUT doReplicaResponse");
        }

        /**
         * @param message
         * @return
         */
        private static String doDelete(String message) {

            Log.d(S_TAG, "IN doDelete");

            Log.d(S_TAG, "doDelete | Delete message : " + message);

            String[] split = message.split(Constants.ESC_DELIMITER);
            String key = split[1];

            int rows = contentResolver.delete(uri, key, null);
            String response = Constants.M_DELETE_RESPONSE + Constants.DELIMITER + rows;

            Log.d(S_TAG, "OUT doDelete");

            return response;
        }

        /**
         * @param message
         * @return
         */
        private static String doDeleteReplica(String message) {

            Log.d(S_TAG, "IN doDeleteReplica");

            Log.d(S_TAG, "doDeleteReplica | Delete replica message : " + message);

            String[] split = message.split(Constants.ESC_DELIMITER);
            String key = split[1];

            int rows = contentResolver.delete(uri, key, new String[]{Constants.M_DELETE_REPLICA});
            String response = Constants.M_DELETE_RESPONSE + Constants.DELIMITER + rows;

            Log.d(S_TAG, "OUT doDeleteReplica");

            return response;
        }

        /**
         * @param message
         * @return
         */
        private static String doDeleteFailure(String message) {

            Log.d(S_TAG, "IN doDeleteFailure");

            Log.d(S_TAG, "doDeleteFailure | Delete Failure message : " + message);

            String[] split = message.split(Constants.ESC_DELIMITER);
            String key = split[1];
            String source = split[2];

            int rows = contentResolver.delete(uri, key, new String[]{Constants.M_DELETE_REPLICA});
            String response = Constants.M_DELETE_RESPONSE + Constants.DELIMITER + key;

            // save it in the delete backup
            // delete the other replica as well.
            Log.d(S_TAG, "Backing up the failure : " + key);

            deleteFailures.add(key);

            Log.d(S_TAG, "Done Backing up the failure : " + key);

            String nextNode = successors[0];
            String howManyTimes = "1";

            Log.d(S_TAG, "Sending delete replica message to next node : " + nextNode);

            deleteReplica(key, howManyTimes);

            Log.d(S_TAG, "Sent delete replica message to next node : " + nextNode);

            Log.d(S_TAG, "OUT doDeleteFailure");

            return response;
        }

        /**
         * @param message
         * @return
         */
        private static String doDeleteReplicaFailure(String message) {

            Log.d(S_TAG, "IN doDeleteReplicaFailure");

            Log.d(S_TAG, "doDeleteReplicaFailure | Delete Replica Failure message : " + message);

            String[] split = message.split(Constants.ESC_DELIMITER);
            String key = split[1];
            String source = split[2];
            String endNode = split[3];

            int rows = contentResolver.delete(uri, key, new String[]{Constants.M_DELETE_REPLICA});
            String response = Constants.M_DELETE_RESPONSE + Constants.DELIMITER + key;

            // save it in the delete backup
            // delete the other replica as well.
            Log.d(S_TAG, "Backing up the failure : " + key);

            deleteFailures.add(key);

            Log.d(S_TAG, "Done Backing up the failure : " + key);

            if (!endNode.equals(myPort)) {

                String nextNode = successors[0];
                String deleteReplicaMessage = buildMessage(new String[]{Constants.M_DELETE_REPLICA, key, source,
                        endNode});

                Log.d(S_TAG, "Sending delete replica message to next node : " + nextNode);

                ClientTask.executeSynchronousWithNoResponse(nextNode, deleteReplicaMessage);

                Log.d(S_TAG, "Sent delete replica message to next node : " + nextNode);

            } else {

                Log.d(S_TAG, "No more delete replication required...");
            }

            Log.d(S_TAG, "OUT doDeleteFailure");

            return response;
        }

        /**
         * @param message
         */
        private static void doGlobalDelete(String message) {

            Log.d(S_TAG, "IN doGlobalDelete");

            Log.d(S_TAG, "doGlobalDelete | Global Delete : " + message);

            contentResolver.delete(uri, Constants.LOCAL, null);

            Log.d(S_TAG, "OUT doGlobalDelete");
        }

        /**
         * @param message
         */
        private static void doGlobalDeleteFailure(String message) {

            Log.d(S_TAG, "IN doGlobalDeleteFailue");

            Log.d(S_TAG, "doGlobalDeleteFailue | Global Delete Failure : " + message);

            deleteFailures.add(Constants.LOCAL);

            Log.d(S_TAG, "OUT doGlobalDeleteFailue");
        }


        /**
         * @param message
         */
        private static String doGlobalQuery(String message) {

            Log.d(S_TAG, "IN doGlobalQuery");

            Log.d(S_TAG, "doGlobalQuery | Global Query : " + message);

            // Query the local database and send the response
            StringBuilder globalQueryResponse = new StringBuilder();
            globalQueryResponse.append(Constants.M_GLOBAL_QUERY_RESPONSE);
            globalQueryResponse.append(Constants.DELIMITER);
            globalQueryResponse.append("JUNK");

            /* References : http://stackoverflow.com/questions/4920528/iterate-through-rows-from-sqlite-query */

            Cursor cursor = contentResolver.query(uri, null, Constants.LOCAL, null, null);
            cursor.moveToFirst();

            int keyIndex = cursor.getColumnIndex(KeyValueContract.KeyValueEntry.KEY);
            int valueIndex = cursor.getColumnIndex(KeyValueContract.KeyValueEntry.VALUE);

            while (!cursor.isAfterLast()) {

                String rowKey = cursor.getString(keyIndex);
                String value = cursor.getString(valueIndex);

                globalQueryResponse.append(Constants.DELIMITER);
                globalQueryResponse.append(rowKey);
                globalQueryResponse.append(Constants.DELIMITER);
                globalQueryResponse.append(value);

                cursor.moveToNext();
            }

            cursor.close();


            Log.d(S_TAG, "OUT doGlobalQuery");

            return globalQueryResponse.toString();
        }


        /**
         * @param message
         */
        private static String doGlobalQueryFailure(String message) {

            Log.d(S_TAG, "IN doGlobalQueryFailure");

            Log.d(S_TAG, "doGlobalQueryFailure | Global Query : " + message);

            // Query the local database and send the response
            StringBuilder globalQueryResponse = new StringBuilder();
            globalQueryResponse.append(Constants.M_GLOBAL_QUERY_RESPONSE);
            globalQueryResponse.append(Constants.DELIMITER);
            globalQueryResponse.append("JUNK");

            /* References : http://stackoverflow.com/questions/4920528/iterate-through-rows-from-sqlite-query */

            String failedNode = getPredecessor(myPort);
            Map<String, String> keyValueMap = backupMap.get(failedNode);

            for (Map.Entry<String, String> entry : keyValueMap.entrySet()) {

                globalQueryResponse.append(Constants.DELIMITER);
                globalQueryResponse.append(entry.getKey());
                globalQueryResponse.append(Constants.DELIMITER);
                globalQueryResponse.append(entry.getValue());

            }

            Log.d(S_TAG, "OUT doGlobalQueryFailure");

            return globalQueryResponse.toString();
        }


        /**
         * @param message
         */
        private static String doQuery(String message) {

            Log.d(S_TAG, "IN doQuery");

            Log.d(S_TAG, "doQuery | Query message : " + message);

            String[] split = message.split(Constants.ESC_DELIMITER);
            String key = split[1];
            String source = split[2];

            String response = Constants.M_QUERY_RESPONSE + Constants.DELIMITER + key +
                    Constants.DELIMITER;

            Cursor cursor;

            queryLock.lock();
            try {


                while (replicationStatusMap.get(key) == null
                        || replicationStatusMap.get(key).equals(Constants.REPLICATION_INCOMPLETE)) {

                    Log.d(TAG, "Acquiring lock in doQuery");
                    try {
                        queryProceeedCond.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            } finally {
                queryLock.unlock();
            }

            Log.d(TAG, "Releasing lock in doQuery. Replication is complete for key : " + key);
            SQLiteOpenHelper helper = DbHelper.getInstance(context);
            SQLiteDatabase readableDatabase = helper.getReadableDatabase();
            cursor = CommonServerProcessingMethods.localQuery(readableDatabase, key);

            if (cursor.isBeforeFirst()) {
                if (cursor.moveToFirst()) {

                    int valueIndex = cursor.getColumnIndex(KeyValueContract.KeyValueEntry.VALUE);
                    String value = cursor.getString(valueIndex);

                    response = response + value;

                    Log.d(S_TAG, "doQuery | Sending result : " + response + " back to : " + source);

                } else {

                    response = response + Constants.NOROW;
                }
            }

            Log.d(S_TAG, "OUT doQuery");

            return response;
        }


        /**
         * @param message
         */
        private static String doQueryFailure(String message) {

            Log.d(S_TAG, "IN doQueryFailure");

            Log.d(S_TAG, "doQueryFailure | Query Failure message : " + message);

            String[] split = message.split(Constants.ESC_DELIMITER);
            String key = split[1];
            String source = split[2];

            String response = Constants.M_QUERY_FAILURE_RESPONSE + Constants.DELIMITER + key +
                    Constants.DELIMITER;

            Cursor cursor;

            SQLiteOpenHelper helper = DbHelper.getInstance(context);
            SQLiteDatabase readableDatabase = helper.getReadableDatabase();
            cursor = CommonServerProcessingMethods.localQuery(readableDatabase, key);

            if (cursor.isAfterLast()) {
                queryLock.lock();
                try {

                    while (replicationStatusMap.get(key) == null
                            || replicationStatusMap.get(key).equals(Constants.REPLICATION_INCOMPLETE)) {

                        Log.d(TAG, "Acquiring lock in doQueryFailure");
                        try {
                            queryProceeedCond.await();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                } finally {
                    queryLock.unlock();
                }

                helper = DbHelper.getInstance(context);
                readableDatabase = helper.getReadableDatabase();
                cursor = CommonServerProcessingMethods.localQuery(readableDatabase, key);
            }

            if (cursor.isBeforeFirst()) {
                if (cursor.moveToFirst()) {

                    int valueIndex = cursor.getColumnIndex(KeyValueContract.KeyValueEntry.VALUE);
                    String value = cursor.getString(valueIndex);

                    response = response + value;

                    Log.d(S_TAG, "doQueryFailure | Sending result : " + response + " back to : " + source);

                } else {

                    response = response + Constants.NOROW;
                }
            }


            Log.d(S_TAG, "doQueryFailure | Sending result : " + response + " back to : " + source);

            Log.d(S_TAG, "OUT doQueryFailure");

            return response;
        }


        /**
         * @param database
         * @param values
         */
        public static void replicate(ContentValues values) {

            Log.d(TAG, "IN replicate");

            String key = (String) values.get(KeyValueContract.KeyValueEntry.KEY);
            String value = (String) values.get(KeyValueContract.KeyValueEntry.VALUE);

            String endNode = (String) values.get(KeyValueContract.KeyValueEntry.REP);
            values.remove(KeyValueContract.KeyValueEntry.REP);

            String source = (String) values.get(KeyValueContract.KeyValueEntry.SOURCE);
            values.remove(KeyValueContract.KeyValueEntry.SOURCE);

            // this is a copy... so do simple insert.. forward the replica
            localInsert(values, key, value);

            CommonServerProcessingMethods.replicateReplica(key, value, endNode, source);

            Log.d(TAG, "OUT replicate");

        }

        /**
         * @param key
         * @param value
         * @param endNode
         * @return
         */
        private static void replicateReplica(String key, String value, String endNode, String source) {

            Log.d(TAG, "IN replicateReplica");

            if (!myPort.equals(endNode)) {

                String replicaMessage = buildMessage(new String[]{Constants.M_REPLICA, key, value, endNode, source});
                Log.d(TAG, "Replicating replica message : " + replicaMessage);

                boolean error = false;
                try {

                    if (failedNode == null) {

                        ClientTask.pingExecuteSynchronousWithNoResponse(endNode, replicaMessage);

                    } else if (failedNode.equals(endNode)) {

                        error = true;

                    } else {

                        ClientTask.pingExecuteSynchronousWithNoResponse(endNode, replicaMessage);
                    }
                } catch (RuntimeException e) {
                    error = true;
                    failedNode = endNode;
                }

                if (error) {

                    Log.e(TAG, "Error occured when replicating the replica message to : " + endNode + ". Message : " +
                            replicaMessage);

                    insertIntoBackupMap(failedNode, key, value);

                    String successorOfFailureNode = getSuccessor(endNode);

                    String replicationFailureMessage = buildMessage(new String[]{Constants.M_REPLICA_FAILURE, key,
                            value, successorOfFailureNode, source, Constants.NO_STORE});

                    Log.d(TAG, "SENDING replication mesage on failure to : " + successorOfFailureNode + ". Message : " +
                            "" + replicationFailureMessage);

                    ClientTask.executeSynchronousWithNoResponse(successorOfFailureNode, replicationFailureMessage);

                    Log.d(TAG, "SENT replication mesage on failure to : " + successorOfFailureNode + ". Message :" +
                            "" + replicationFailureMessage);
                }

            } else {


                replicationStatusMap.put(key, Constants.REPLICATION_COMPLETE);
                try {

                    queryLock.lock();
                    queryProceeedCond.signalAll();

                } finally {
                    queryLock.unlock();
                }

                String replicaResponse = buildMessage(new String[]{Constants.M_REPLICA_RESPONSE, key, value});
                if (source != null) {
                    ClientTask.executeSynchronousWithNoResponse(source, replicaResponse);
                }
                Log.d(TAG, "End of replication for key : " + key);
            }

            Log.d(TAG, "OUT replicateReplica");

        }

        /**
         * @param key
         * @return
         */
        public static Cursor localQuery(SQLiteDatabase readableDatabase, String key) {

            String extractQuery;
            Cursor cursor;

            extractQuery = "SELECT * FROM " + KeyValueContract.KeyValueEntry.TABLE_NAME + " WHERE key='" +
                    key + "'";

            Log.d(TAG, extractQuery);

            cursor = readableDatabase.rawQuery(extractQuery, null);
            return cursor;
        }

    }

    /*

    References : http://stackoverflow.com/questions/2120248/how-to-synchronize-a-static-variable-among-threads
    -running-different-instances-o
     */

    private static boolean isReset = false;
    private static final Object lockObject = new Object();

    public static void setReset(boolean value) {
        synchronized (lockObject) {
            isReset = value;
        }
    }

    public static boolean getReset() {
        boolean retValue;
        synchronized (lockObject) {
            retValue = isReset;
        }
        return retValue;
    }
}
