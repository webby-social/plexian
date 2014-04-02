package net.sf.plexian.store;


import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

import net.sf.plexian.PlexianConstants;


public abstract class Block {

    private BlockSizeOfType sizeOfType;
    private FileSpaceManager fileSpaceManager;

    private int size;
    private long pos;
    
    protected Block(FileSpaceManager fileSpaceManager, long pos, BlockSizeOfType sizeOfType) {
        this.fileSpaceManager = fileSpaceManager;
        this.pos = pos;
        this.sizeOfType = sizeOfType;
    }

    public long getPos() {
        return pos;
    }

    public void setPos(long pos) {
        this.pos = pos;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    protected FileSpaceManager getFileSpaceManager() {
        return fileSpaceManager;
    }
    
    protected void clear() {
        fileSpaceManager = null;
    }
    
    protected void load() throws IOException {
        RandomAccessFile index = fileSpaceManager.getIndexFileProvider().open();
        index.seek(pos);
        deserializeFromStream(readFromFile(index));
    }
    
    private boolean ensureSpace() throws IOException {
        int requiredSize = calculateSize();
        if (requiredSize > sizeOfType.maxSizeValue()) {
            throw new IndexOutOfBoundsException("calculated size greater then max block size");
        }
        long newPos = fileSpaceManager.reallocate(pos, size, requiredSize);
        boolean reallocated = newPos != pos;
        pos = newPos;
        size = requiredSize;
        return reallocated;
    }
    
    protected void store() throws IOException {
        while(ensureSpace());
        RandomAccessFile index = fileSpaceManager.getIndexFileProvider().open();
        long oldFilePointer = index.getFilePointer();
        index.seek(pos);
        index.write(serialize(size));
        index.seek(oldFilePointer);
    }
    
    protected DataInputStream readFromFile(RandomAccessFile index) throws IOException {
        byte[] block = new byte[PlexianConstants.READ_BLOCK_SIZE];
        index.read(block);
        ByteArrayInputStream bi = new ByteArrayInputStream(block);
        DataInputStream dis = new DataInputStream(bi);
        int sz = sizeOfType.readSize(dis);
        if (sz < 0) {
            throw new IOException("bad file size less then zero");
        }
        if (sz > PlexianConstants.READ_BLOCK_SIZE) {
            byte[] newBlock = new byte[sz];
            index.read(newBlock, PlexianConstants.READ_BLOCK_SIZE, sz-PlexianConstants.READ_BLOCK_SIZE);
            System.arraycopy(block, 0, newBlock, 0, PlexianConstants.READ_BLOCK_SIZE);
            bi.close();
            bi = new ByteArrayInputStream(newBlock);
            dis = new DataInputStream(bi);
            dis.skip(sizeOfType.getSize());
        }
        this.size = sz;
        return dis;
    }
    
    public abstract int calculateSize();

    public abstract void deserializeFromStream(DataInputStream index) throws IOException;
    
    public abstract byte[] serialize(int size) throws IOException;
}
