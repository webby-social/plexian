package net.sf.plexian.springframework;


import java.util.List;

import net.sf.plexian.SearchType;
import net.sf.plexian.impl.PlexianImpl;

import org.springframework.beans.factory.DisposableBean;


public class PlexianBean extends PlexianImpl implements DisposableBean {

    private boolean useJmx = false;
    private volatile int searchOperations = 0;
    private volatile long sumSearchMills = 0;
    private volatile long maxSearchMills = 0;
    
    public PlexianBean(String directory) {
        super(directory);
    }
    
    public void destroy() throws Exception {
        closeAll();
    }
    
    public List<String> search(String field, SearchType type, String startsWith) {
        long tm0 = useJmx ? System.currentTimeMillis() : 0;
        List<String> result = super.search(field, type, startsWith);
        if (useJmx && result.size() > 0) {
            long searchMillis = System.currentTimeMillis() - tm0;
            searchOperations++;
            sumSearchMills += searchMillis;
            if (searchMillis > maxSearchMills) {
                maxSearchMills = searchMillis;
            }
        }
        return result;
    }
    
    public int getBatchSize() {
        return batchSize;
    }
    
    public long getUnusedMillis() {
        return unusedMillis;
    }

    public void setUseJmx(boolean useJmx) {
        this.useJmx = useJmx;
    }

    public int getSearchOperations() {
        return searchOperations;
    }

    public long getSumSearchMills() {
        return sumSearchMills;
    }

    public long getMaxSearchMills() {
        return maxSearchMills;
    }
    
    public int getNumCloses() {
        return closeOperations;
    }
    
    public int getNumFlushes() {
        return flushOperations;
    }

}
