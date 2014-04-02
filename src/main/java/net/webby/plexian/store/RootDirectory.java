package net.webby.plexian.store;


import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

import net.webby.plexian.PlexianConstants;


public class RootDirectory extends Block {

    private static final int MAGIC = 0x4444;
    private static final int ROOTDIR_SIZE = Short.SIZE / 8;
    private static final int MAGIC_SIZE = Short.SIZE / 8;
    private static final int FREEDIR_POS_SIZE = PlexianConstants.POINTER_SIZE;
    private static final int ROOTNODE_POS_SIZE = PlexianConstants.POINTER_SIZE;
    private static final int TOTAL_ITEMS_SIZE = Long.SIZE / 8;
    
    private FreeSpaceDirectory freeSpaceDirectory;
    private Node rootNode;
    private long totalItems = 0;
    
    protected RootDirectory(FileSpaceManager fileSpaceManager) throws IOException {
        super(fileSpaceManager, 0, BlockSizeOfType.SHORT);
        RandomAccessFile index = fileSpaceManager.getIndexFileProvider().open();
        if (index.length() == 0) {
            freeSpaceDirectory = new FreeSpaceDirectory(fileSpaceManager, 0);
            rootNode =  new Node(fileSpaceManager, null, 0);
            store();
        }
        else {
            load();
        }
    }
    
    public int calculateSize() {
        return ROOTDIR_SIZE + MAGIC_SIZE + ROOTNODE_POS_SIZE + FREEDIR_POS_SIZE + TOTAL_ITEMS_SIZE;
    }
    
    public void deserializeFromStream(DataInputStream index) throws IOException {
        int magic = index.readShort();
        if (magic != MAGIC) {
            throw new IOException("invalid magic");
        }
        long rootNodePos = PointerUtils.readPointer(index);
        if (rootNodePos < 0) {
            throw new IOException("invalid root node pos");
        }
        rootNode = new Node(getFileSpaceManager(), null, rootNodePos);
        long freeDirPos = PointerUtils.readPointer(index);
        if (freeDirPos < 0) {
            throw new IOException("invalid free dir pos");
        }
        freeSpaceDirectory = new FreeSpaceDirectory(getFileSpaceManager(), freeDirPos);   
        totalItems = index.readLong();
        if (totalItems < 0) {
            throw new IOException("invalid total items");
        }        
    }
    
    public byte[] serialize(int size) throws IOException {
        ByteArrayOutputStream ba = new ByteArrayOutputStream(size);
        DataOutputStream index = new DataOutputStream(ba);
        index.writeShort(size);
        index.writeShort(MAGIC);
        long rootNodePos = rootNode.persist();
        if (rootNodePos <= 0) {
            throw new IOException("invalid root node pos");
        }
        PointerUtils.writePointer(index, rootNodePos);
        long freeDirPos = freeSpaceDirectory.persist();
        PointerUtils.writePointer(index, freeDirPos);
        index.writeLong(totalItems);
        ba.close();
        return ba.toByteArray();
    }

    public FreeSpaceDirectory getFreeSpaceDirectory() {
        return freeSpaceDirectory;
    }

    public Node getRootNode() {
        return rootNode;
    }
    
    public void clear() {
        freeSpaceDirectory.clear();
        rootNode.clear();
        freeSpaceDirectory = null;
        rootNode = null;
    }

    public long getTotalItems() {
        return totalItems;
    }
    
    public void addTotalItems(int num) {
        totalItems += num;
    }
}
