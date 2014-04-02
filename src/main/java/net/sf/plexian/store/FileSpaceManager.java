package net.sf.plexian.store;


import java.io.IOException;
import java.io.RandomAccessFile;

import net.sf.plexian.PlexianConstants;


public class FileSpaceManager {

    private IndexFileProvider indexFileProvider;
    private FreeSpaceDirectoryProvider freeSpaceDirectoryProvider;
    
    protected FileSpaceManager(IndexFileProvider indexFileProvider, FreeSpaceDirectoryProvider freeSpaceDirectoryProvider, long freeDirPos) throws IOException {
        this.indexFileProvider = indexFileProvider;
        this.freeSpaceDirectoryProvider = freeSpaceDirectoryProvider;
    }
    
    protected IndexFileProvider getIndexFileProvider() {
        return indexFileProvider;
    }
    
    protected long reallocate(long pos, int size, int requiredSize) throws IOException {
        int existsBlocks = calculateNumberOfBlocks(size);
        int requiredBlocks = calculateNumberOfBlocks(requiredSize);
        if (size != 0 && requiredBlocks <= existsBlocks) {
            return pos;
        }
        else {
            FreeSpaceDirectory dir = freeSpaceDirectoryProvider.getFreeSpaceDirectory();
            if (pos != 0 && dir != null) {
                dir.addFreeSpace(pos, existsBlocks);
            }
            Long newPos = (dir == null) ? null : dir.getFreeSpace(requiredBlocks);
            if (newPos == null) {
                return allocateBlocks(requiredBlocks);
            }
            return newPos;
        }
    }
    
    protected void deleteAllocation(long pos, int size) throws IOException {
        int existsBlocks = calculateNumberOfBlocks(size);
        if (pos != 0) {
            FreeSpaceDirectory dir = freeSpaceDirectoryProvider.getFreeSpaceDirectory();
            if (dir != null) {
                dir.addFreeSpace(pos, existsBlocks);
            }
        }
    }
    
    private long allocateBlocks(int expectedBlocks) throws IOException {
        RandomAccessFile index = indexFileProvider.open();
        long oldFilePointer = index.getFilePointer();
        long allocatedPos = index.length();
        index.seek(allocatedPos);
        fillBlocksByZero(index, expectedBlocks);
        index.seek(oldFilePointer);
        return allocatedPos;
    }
    
    private void fillBlocksByZero(RandomAccessFile index, int blocks) throws IOException {
        for (int i = 0; i < blocks; ++i) {
            index.write(PlexianConstants.ZERO_BLOCK);
        }
    }
    
    protected static int calculateNumberOfBlocks(int size) {
        int blocks = size / PlexianConstants.STORE_BLOCK_SIZE;
        if (size % PlexianConstants.STORE_BLOCK_SIZE != 0) {
            blocks++;
        }
        return blocks;
    }
}
