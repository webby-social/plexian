package net.sf.plexian.store;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.sf.plexian.PlexianConstants;


public class NodeQueue {

    private List<String> queue = new ArrayList<String>(PlexianConstants.MAX_QUERY_ENTRIES);;
    
    public int calculateSize() {
        int size = 0;
        for (String name : queue) {
            size += StringStorageUtils.calculateStringSize(name);
        }
        return size;
    }
    
    public void serialize(DataOutput index) throws IOException {
        for (String name : queue) {
            StringStorageUtils.writeString(index, name);
        }
    }
    
    public void deserializeFromStream(DataInput index, int queueItems) throws IOException {
        queue.clear();
        for (int i = 0; i < queueItems; ++i) {
            queue.add(StringStorageUtils.readString(index));
        }
    }
    
    public List<String> getEntries(String beginString) {
        List<String> list = new ArrayList<String>(PlexianConstants.MAX_QUERY_ENTRIES);
        for (String ending : queue) {
            list.add(beginString + ending);
        }
        return list;
    }
    
    public void clear() {
        queue.clear();
    }
    
    public int size() {
        return queue.size();
    }
    
    public boolean isEmpty() {
        return queue.isEmpty();
    }
    
    public void remove(String name) {
        queue.remove(name);
    }
    
    public void update(Node node, String part, int weight) throws IOException {
        if (queue.size() > 0) {
            queue.remove(part);
            Integer i = findLessProiritizedElement(node, weight);
            if (i != null) {
                queue.add(i, part);
                if (queue.size() > PlexianConstants.MAX_QUERY_ENTRIES) {
                    queue.remove(queue.size()-1);
                }                
            }
            else if (queue.size() < PlexianConstants.MAX_QUERY_ENTRIES) {
                queue.add(part);
            }
        }
        else {
            queue.add(part);
        }
    }
    
    private Integer findLessProiritizedElement(Node node, int weight) throws IOException {
        if (queue != null) {
            for (int i = 0; i != queue.size(); ++i) {
                if (node.getWeight(queue.get(i), 0) <= weight) {
                    return i;
                }
            }
        }
        return null;
    }
}
