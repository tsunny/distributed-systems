package edu.buffalo.cse.cse486586.simpledht;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by sunny on 3/25/16.
 */
public class GlobalRepo {

    private static final Map<String, String> nodeIdMap = new HashMap<String, String>();

    static {

        nodeIdMap.put("5554", "11108");
        nodeIdMap.put("5556", "11112");
        nodeIdMap.put("5558", "11116");
        nodeIdMap.put("5560", "11120");
        nodeIdMap.put("5562", "11124");
    }

    public static String getPortNumber(String nodeId) {
        return nodeIdMap.get(nodeId);
    }
}
