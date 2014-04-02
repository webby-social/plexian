package net.webby.plexian.store;

import java.io.DataInputStream;
import java.io.IOException;

public enum BlockSizeOfType {
    SHORT, INTEGER;
    
    public int maxSizeValue() {
        switch(this) {
        case SHORT:
            return (int)Short.MAX_VALUE;
        case INTEGER:
            return Integer.MAX_VALUE;
        }
        return 0;
    }
    
    public int getSize() {
        switch(this) {
        case SHORT:
            return Short.SIZE / 8;
        case INTEGER:
            return Integer.SIZE / 8;
        }
        return 0;
    }
    
    public int readSize(DataInputStream dis) throws IOException {
        switch(this) {
        case SHORT:
            return (int)dis.readShort();
        case INTEGER:
            return dis.readInt();
        }
        return 0;
    }
}
