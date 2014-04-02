package net.webby.plexian.jmx;

public interface PlexianMBean {

    public int getNumFileds();
    
    public int getNumReadonlyFields();
    
    public int getNumBadFileds();
    
    public int getBatchSize();
    
    public long getUnusedMillis();
    
    public int getNumSearches();
    
    public long getAvgSearchMillis();
    
    public long getMaxSearchMillis();
    
    public int getNumFlushes();
    
    public int getNumCloses();
    
    public int getLoadedNodes();
    
    public void setBatchSize(int batchSize);
    
    public void setUnusedMillis(long millis);
    
}
