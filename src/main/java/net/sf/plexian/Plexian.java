package net.sf.plexian;


import java.util.List;

import net.sf.plexian.store.OriginalName;


public interface Plexian {
    
    public List<String> search(String field, SearchType type, String startsWith);
    
    public Integer[] getFreqs(String field, List<String> texts);
    
    public List<OriginalName> getSourceTexts(String field, String lowerCaseText);
    
    public void index(String field, String text, IndexType type, int freq);
    
    public long getIndexedItems(String field);
    
    public void remove(String field, String text);
    
    public void remove(String field);
    
    public void flush(String field);
    
    public void flushAll();

    public void close(String field);
    
    public void closeAll();
    
    public void closeUnused();
    
    public boolean isBad(String field);
    
    public boolean isReadOnly(String field);
    
    public void setBatchSize(int batchSize);
    
    public void setUnusedMillis(long unusedMillis);
    
}
