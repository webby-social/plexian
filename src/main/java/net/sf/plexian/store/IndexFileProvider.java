package net.sf.plexian.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import net.sf.plexian.PlexianConstants;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class IndexFileProvider {

    private final Log log = LogFactory.getLog(this.getClass());
    
    private String directory;
    private boolean readonly;
    private RandomAccessFile index;
    
    protected IndexFileProvider(String directory, boolean readonly) {
        this.directory = directory;
        this.readonly = readonly;
    }
    
    private void ensureIndexFileExists() throws IOException {
        File index = new File(directory + File.separatorChar + PlexianConstants.INDEX_FILE);
        if (!index.exists()) {
            index.createNewFile();
        }
    }
    
    public boolean isOpen() {
        return index != null;
    }
    
    public RandomAccessFile open() throws IOException {
        if (index == null) {
            ensureIndexFileExists();
            index = new RandomAccessFile(directory + File.separatorChar + PlexianConstants.INDEX_FILE, readonly ? "r" : "rw");
            if (!readonly) {
                DirectoryLocker.lock(directory);
            }
        }
        return index;
    }
    
    public void close() {
        if (index != null) {
            try {
                index.close();
                if (!readonly) {
                    DirectoryLocker.unlock(directory);
                }
            }
            catch(IOException e) {
                log.error("close index fail", e);
            }
            index = null;
        }
    }    
}
