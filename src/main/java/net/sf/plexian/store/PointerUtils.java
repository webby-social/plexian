package net.sf.plexian.store;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import net.sf.plexian.PlexianConstants;


public class PointerUtils {

    public static long readPointer(DataInput index) throws IOException {
        if (PlexianConstants.POINTER_SIZE == (Integer.SIZE / 8)) {
            return (long)index.readInt();
        }
        else if (PlexianConstants.POINTER_SIZE == (Long.SIZE / 8)) {
            return index.readLong();
        }
        else {
            throw new IOException("invalid constant pointer size");
        }
    }
    
    public static void writePointer(DataOutput index, long pointer) throws IOException {
        if (PlexianConstants.POINTER_SIZE == (Integer.SIZE / 8)) {
            if (pointer > (long)Integer.MAX_VALUE) {
                throw new IOException("pointer greater max int value");
            }            
            index.writeInt((int)pointer);
        }
        else if (PlexianConstants.POINTER_SIZE == (Long.SIZE / 8)) {
            index.writeLong(pointer);
        }
        else {
            throw new IOException("invalid constant pointer size");
        }
    }
}
