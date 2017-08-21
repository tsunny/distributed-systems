package edu.buffalo.cse.cse486586.simpledynamo;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

public class Utils {

    /**
     * @param input
     * @return
     */
    public static String genHash(String input) {

        try {

            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            byte[] sha1Hash = sha1.digest(input.getBytes());
            Formatter formatter = new Formatter();
            for (byte b : sha1Hash) {
                formatter.format("%02x", b);
            }
            return formatter.toString();

        } catch (NoSuchAlgorithmException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }

    public static int mod(int value, int base) {

        return (((value) % base) + base) % base;
    }
}
