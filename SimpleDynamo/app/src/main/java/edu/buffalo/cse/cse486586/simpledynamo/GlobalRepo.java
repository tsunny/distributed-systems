package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentResolver;
import android.net.Uri;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

public class GlobalRepo {


    private static Uri uri = null;
    private static String myPort = null;
    private static String myHash = null;

    public static List<String> ring = new ArrayList<String>();
    public static Map<String, String> reverseLookup = new HashMap<String, String>();
    public static String[] successors = new String[]{};

    private static String avd0 = "5554";

    private static ContentResolver contentResolver = null;


    private static final Map<String, String> nodeIdMap = new HashMap<String, String>();


    static {

        nodeIdMap.put("5554", "11108");
        nodeIdMap.put("5556", "11112");
        nodeIdMap.put("5558", "11116");
        nodeIdMap.put("5560", "11120");
        nodeIdMap.put("5562", "11124");
    }

    public static Uri getUri() {
        return uri;
    }

    public static void setUri(Uri uri) {
        GlobalRepo.uri = uri;
    }

    public static String getMyPort() {
        return myPort;
    }

    public static void setMyPort(String myPort) {
        GlobalRepo.myPort = myPort;
    }

    public static String getMyHash() {
        return myHash;
    }

    public static void setMyHash(String myHash) {
        GlobalRepo.myHash = myHash;
    }

    public static ContentResolver getContentResolver() {
        return contentResolver;
    }

    public static void setContentResolver(ContentResolver contentResolver) {
        GlobalRepo.contentResolver = contentResolver;
    }

    public static String getPortNumber(String nodeId) {
        return nodeIdMap.get(nodeId);
    }
}
