package net.webby.plexian.store;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import net.webby.plexian.PlexianConstants;


public class StringStorageUtils {

    private static final int STRING_SIZE = Short.SIZE / 8;
    
    public static int calculateStringSize(String name) {
        try {
            int sizeInByted = name.getBytes(PlexianConstants.STORE_CHARSET).length;
            if (sizeInByted >= (int)Short.MAX_VALUE) {
                sizeInByted = (int)Short.MAX_VALUE-1;
            }
            return STRING_SIZE + sizeInByted;
        }
        catch(UnsupportedEncodingException e) {
            return STRING_SIZE;
        }
    }
    
    public static void writeString(DataOutput index, String name) throws IOException {
        byte[] blob = name.getBytes(PlexianConstants.STORE_CHARSET);
        int sizeInByted = blob.length;
        if (sizeInByted >= (int)Short.MAX_VALUE) {
            sizeInByted = (int)Short.MAX_VALUE-1;
        }
        index.writeShort(sizeInByted);
        index.write(blob, 0, sizeInByted);
    }
    
    public static String readString(DataInput index) throws IOException {
        int size = index.readShort();
        byte[] blob = new byte[size];
        index.readFully(blob);
        return new String (blob, PlexianConstants.STORE_CHARSET);
    }
    
}
