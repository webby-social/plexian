package net.webby.plexian.store;


import java.io.File;
import java.io.IOException;

import net.webby.plexian.PlexianConstants;


public class DirectoryLocker {

    public static boolean isReadOnly(String directory) throws IOException {
        String lockFile = directory + File.separatorChar + PlexianConstants.READONLY_LOCK_FILE;
        File lock = new File(lockFile);
        return lock.exists();
    }
    
    public static void lock(String directory) throws IOException {
        String lockFile = directory + File.separatorChar + PlexianConstants.INDEX_LOCK_FILE;
        File lock = new File(lockFile);
        if (lock.exists()) {
            throw new IOException("directory " + directory + " has been locked for write");
        }
        if (!lock.createNewFile()) {
            throw new IOException("can't lock directory " + directory + " for write");
        }
    }

    public static boolean unlock(String directory) throws IOException {
        boolean wasLocked = false;
        File dir = new File(directory);
        if (dir.exists() && dir.isDirectory()) {
            String lockFile = dir.getAbsolutePath() + File.separatorChar + PlexianConstants.INDEX_LOCK_FILE;
            File lock = new File(lockFile);
            if (lock.exists()) {
                wasLocked = true;
                lock.delete();
            }
        }
        return wasLocked;
    }

}
