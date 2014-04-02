package net.sf.plexian.impl;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.plexian.IndexType;
import net.sf.plexian.Plexian;
import net.sf.plexian.PlexianConstants;
import net.sf.plexian.SearchType;
import net.sf.plexian.store.Container;
import net.sf.plexian.store.ContainerType;
import net.sf.plexian.store.DirectoryLocker;
import net.sf.plexian.store.OriginalName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class PlexianImpl implements Plexian {

    private final Log log = LogFactory.getLog(this.getClass());

    protected int batchSize = PlexianConstants.DEFAULT_BATCH_SIZE;
    protected long unusedMillis = PlexianConstants.DEFAULT_UNUSED_MILLIS;
    private String directory;
    private Map<String, Container> fields = new HashMap<String, Container>();
    private Set<String> badFields = new HashSet<String>();
    protected volatile int closeOperations = 0;
    protected volatile int flushOperations = 0;
    
    public PlexianImpl(String directory) {
        this.directory = directory;
        File dir = new File(directory);
        if (dir.exists() && !dir.isDirectory()) {
            log.error("not a directory " + directory);
        }
        if (!dir.exists() && !dir.mkdir()) {
            log.error("can't create directory " + directory);
        }
        for (String name : dir.list()) {
            try {
                File containerDir = new File(directory + File.separatorChar + name);
                if (containerDir.exists() && containerDir.isDirectory()) {
                    if (DirectoryLocker.isReadOnly(containerDir.getAbsolutePath())) {
                        fields.put(containerDir.getName(), new Container(containerDir.getAbsolutePath(), ContainerType.READONLY));
                    }
                    else {
                        if (DirectoryLocker.unlock(containerDir.getAbsolutePath())) {
                            badFields.add(containerDir.getName());
                        }
                        fields.put(containerDir.getName(), new Container(containerDir.getAbsolutePath(), ContainerType.WRITE));
                    }
                }
            }
            catch(IOException e) {
                log.error("fail to unnlock container", e);
            }
        }
    }
    
    public boolean isBad(String field) {
        synchronized(badFields) {
            return badFields.contains(field);
        }
    }
    
    public void remove(String field, String text) {
        Container container = getContainer(field, false);
        if (container != null) {
            synchronized(container) {
                try {
                    container.remove(text);
                    afterOperations(container);
                }
                catch(IOException e) {
                    log.error("fail remove", e);
                }
            }
        }
    }
    
    public void remove(String field) {
        synchronized(fields) {
            Container container = fields.remove(field);
            if (container != null) {
                synchronized(container) {
                    container.setReadOnly();
                    container.close();
                    closeOperations++;
                }
            }
            try {
                fields.put(field, new Container(directory + File.separatorChar + field, ContainerType.NEW));
            }
            catch(IOException e) {
                log.error("fail to create field", e);
            }  
        }
    }
    
    public List<String> search(String field, SearchType type, String startsWith) {
        List<String> result = Collections.emptyList();
        Container container = getContainer(field, false);
        if (container != null) {
            synchronized(container) {
                try {
                    if (type == SearchType.ORIGINAL_NAMES) {
                        result = container.search(startsWith);
                    }
                    else if (type == SearchType.LOWERCASE_NAMES) {
                        result = container.searchLowerCase(startsWith);
                    }
                    else if (type == SearchType.KEYWORDS) {
                        result = container.searchByKeywords(startsWith);
                    }                    
                    afterOperations(container);
                }
                catch(IOException e) {
                    log.error("fail search", e);
                }
            }
        }
        return result;
    }
    
    public Integer[] getFreqs(String field, List<String> texts) {
        Integer[] result = new Integer[texts.size()];
        Container container = getContainer(field, false);
        if (container != null) {
            synchronized(container) {
                for (int i = 0; i < texts.size(); ++i) {
                    try {
                        result[i] = container.getWeight(texts.get(i));
                    }
                    catch(IOException e) {
                        log.error("fail to get weight", e);
                    }
                }
                afterOperations(container);
            }
        }
        return result;
    }
    
    public List<OriginalName> getSourceTexts(String field, String lowerCaseText) {
        List<OriginalName> result = Collections.emptyList();
        Container container = getContainer(field, false);
        if (container != null) {
            synchronized(container) {
                try {
                    result = container.getOriginalNames(lowerCaseText);
                }
                catch(IOException e) {
                    log.error("fail to get source text", e);
                }
                afterOperations(container);
            }
        }
        return result;
    }
    
    public void index(String field, String text, IndexType type, int freq) {
        Container container = getContainer(field, true);
        if (container != null) {
            synchronized(container) {
                try {
                    if (type == IndexType.ADD_FREQ) {
                        container.addWeight(text, freq);
                    }
                    else if (type == IndexType.SET_FREQ) {
                        container.setWeight(text, freq);
                    }
                    afterOperations(container);
                }
                catch(IOException e) {
                    log.error("index fail", e);
                }
            }
        }
    }

    public long getIndexedItems(String field) {
        Container container = getContainer(field, false);
        if (container != null) {
            synchronized(container) {
                try {
                    return container.getIndexedItems();
                }
                catch(IOException e) {
                    log.error("get indexed items fail", e);
                }
            }
        }
        return 0L;
    }
    
    public void flush(String field) {
        Container container = getContainer(field, false);
        if (container != null) {
            synchronized(container) {
                container.flush();
                flushOperations++;
            }
        }
    }
    
    public void flushAll() {
        for (Container container : getContainers()) {
            synchronized(container) {
                container.flush();
                flushOperations++;
            }
        }
    }
    
    public void close(String field) {
        Container container = getContainer(field, false);
        if (container != null) {
            synchronized(container) {
                container.close();
                closeOperations++;
            }
        }
    }

    public void closeAll() {
        for (Container container : getContainers()) {
            synchronized(container) {
                container.close();
                closeOperations++;
            }
        }
    }
    
    public void closeUnused() {
        long currentMilliseconds = System.currentTimeMillis();
        List<Container> containers = new ArrayList<Container>();
        synchronized(fields) {
            for (String name : fields.keySet()) {
                Container container = fields.get(name);
                if (container != null) {
                    synchronized(container) {
                        if (container.getLastAccessTime() + unusedMillis < currentMilliseconds) {
                            containers.add(container);
                        }
                    }
                }
            }
        }
        for (Container container : containers) {
            synchronized(container) {
                container.close();
                closeOperations++;
            }
        }
    }
    
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }    
    
    private List<Container> getContainers() {
        List<Container> containers = new ArrayList<Container>();
        synchronized(fields) {
            for (String name : fields.keySet()) {
                Container container = fields.get(name);
                if (container != null) {
                    containers.add(fields.get(name));
                }
            }
        }
        return containers;
    }

    private Container getContainer(String field, boolean createIfNotExists) {
        Container container = null;
        synchronized(fields) {
            container = fields.get(field);
            if (container == null && createIfNotExists) {
                try {
                    container = new Container(directory + File.separatorChar + field, ContainerType.WRITE);
                    fields.put(field, container);
                }
                catch(IOException e) {
                    log.error("field was locked", e);
                }
            }
        }
        return container;
    }       

    public Set<String> getAllFields() {
        synchronized(fields) {
            return new LinkedHashSet<String>(fields.keySet()); 
        }
    }
    
    public Set<String> getBadFields() {
        synchronized(badFields) {
            return new LinkedHashSet<String>(badFields); 
        }
    }    
    
    public int countLoadedNodes(String field) {
        int nodes = 0;
        Container container = getContainer(field, false);
        if (container != null) {
            synchronized(container) {
                try {
                    nodes = container.countLoadedNodes();
                }
                catch(IOException e) {
                    log.error("count nodes fail", e);
                }
            }
        }
        return nodes;
    }
    
    public boolean isReadOnly(String field) {
        boolean readonly = false;
        Container container = getContainer(field, false);
        if (container != null) {
            synchronized(container) {
                readonly = container.isReadOnly();
            }
        }
        return readonly;
    }
    
    private void afterOperations(Container container) {
        if (batchSize > 0 && container.getOperations() >= batchSize) {
            container.flush();
            flushOperations++;
        }
    }

    public void setUnusedMillis(long unusedMillis) {
        this.unusedMillis = unusedMillis;
    }
    
}
