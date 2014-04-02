package net.sf.plexian.store;


import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.plexian.PlexianConstants;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class FreeSpaceDirectory extends Block {

    private final Log log = LogFactory.getLog(this.getClass());
    
    private static final int MAGIC = 0x3333;
    private static final int FREEDIR_SIZE = Integer.SIZE / 8;
    private static final int MAGIC_SIZE = Short.SIZE / 8;
    private static final int ENTRIES_COUNT_SIZE = Short.SIZE / 8;
    private static final int BLOCKS_SIZE = Short.SIZE / 8;
    private static final int POS_COUNT_SIZE = Integer.SIZE / 8;
    private static final int POS_SIZE = PlexianConstants.POINTER_SIZE;
    
    private Map<Integer, List<Long>> freeSpace = new HashMap<Integer, List<Long>>();
    
    private boolean persists;
    
    protected FreeSpaceDirectory(FileSpaceManager fileSpaceManager, long pos) throws IOException {
        super(fileSpaceManager, pos, BlockSizeOfType.INTEGER);
        if (pos != 0) {
            load();
        }
        persists = true;
    }
    
    public long persist() throws IOException {
        if (!persists) {
            store();
            persists = true;
        }        
        return getPos();
    }
    
    private int calculateFreeEntriesSize() {
        int size = 0;
        for (Integer blocks : freeSpace.keySet()) {
            int count = freeSpace.get(blocks).size();
            if (count > 0) {
                size += BLOCKS_SIZE + POS_COUNT_SIZE + POS_SIZE * count;
            }
        }
        return size;
    }
    
    public int calculateSize() {
        int size = FREEDIR_SIZE + MAGIC_SIZE;
        size += ENTRIES_COUNT_SIZE + calculateFreeEntriesSize();
        return size;
    }
    
    private void eraseEmptyEntries() {
        for (Integer blocks : new LinkedList<Integer>(freeSpace.keySet())) {
            List<Long> list = freeSpace.get(blocks);
            if (list == null || list.size() == 0) {
                freeSpace.remove(blocks);
            }
        }
    }
    
    public void deserializeFromStream(DataInputStream index) throws IOException {
        int magic = index.readShort();
        if (magic != MAGIC) {
            throw new IOException("invalid magic");
        }
        int entries = index.readShort();
        if (entries < 0) {
            throw new IOException("bad file entries less then zero");
        }
        freeSpace.clear();
        for (int i = 0; i < entries; ++i) {
            int blocks = index.readShort();
            if (blocks < 0) {
                throw new IOException("bad file blocks less then zero");
            }
            if (freeSpace.get(blocks) != null) {
                throw new IOException("bad file blocks double in map");
            }
            int posCount = index.readInt();
            if (posCount < 0) {
                throw new IOException("bad file posCount less then zero");
            }
            List<Long> posList = new ArrayList<Long>(posCount);
            for (int j = 0; j < posCount; ++j) {
                posList.add(PointerUtils.readPointer(index));
            }
            freeSpace.put(blocks, posList);
        }
    }
    
    public byte[] serialize(int size) throws IOException {
        ByteArrayOutputStream ba = new ByteArrayOutputStream(size);
        DataOutputStream index = new DataOutputStream(ba);
        index.writeInt(size);
        index.writeShort(MAGIC);
        eraseEmptyEntries();
        Set<Integer> entries = freeSpace.keySet();
        if (entries.size() > (int)Short.MAX_VALUE) {
            throw new IOException("entries.size greater max short value");
        } 
        index.writeShort(entries.size());
        for (Integer blocks : entries) {
            if (blocks <= (int)Short.MAX_VALUE) {
                index.writeShort(blocks);
                List<Long> posList = freeSpace.get(blocks);
                index.writeInt(posList.size());
                for (Long pos : posList) {
                    PointerUtils.writePointer(index, pos);
                }
            }
            else {
                log.error("blocks greater max short value");
            }
        }
        ba.close();
        return ba.toByteArray();
    }
    
    protected void addFreeSpace(long pos, int blocks) {
        List<Long> list = freeSpace.get(blocks);
        if (list == null) {
            list = new LinkedList<Long>();
            freeSpace.put(blocks, list);
        }
        list.add(pos);
        persists = false;
    }
    
    protected Long getFreeSpace(int blocks) {
        for (int i = 0; i < PlexianConstants.STORE_BLOCK_SEARCH_ITEMS; ++i) {
            List<Long> list = freeSpace.get(blocks + i);
            if (list != null && list.size() > 0) {
                Long pos = list.remove(0);
                if (i > 0) {
                    addFreeSpace(pos, i);
                }
                persists = false;
                return pos + PlexianConstants.STORE_BLOCK_SIZE * i;
            }
        }
        return null;
    }    
}
