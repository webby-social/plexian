package net.sf.plexian.indexator;

import java.util.List;
import java.util.Set;

public interface PlexianIndexators {

    public Set<String> getRunningFields();
    
    public List<String> getSupportedFields();
        
	public void reindex(String field);
}
