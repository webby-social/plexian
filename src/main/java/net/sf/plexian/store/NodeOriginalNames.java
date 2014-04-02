package net.sf.plexian.store;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NodeOriginalNames {

    private static final int ORNAME_WEIGHT_SIZE = Integer.SIZE / 8;
    
    private Map<String, Integer> originalNames = new HashMap<String, Integer>();;

    public int calculateSize() {
        int size = 0;
        if (originalNames != null) {
            for (String originalName : originalNames.keySet()) {
                size += StringStorageUtils.calculateStringSize(originalName);
                size += ORNAME_WEIGHT_SIZE;
            }
        }
        return size;
    }
    
    public void serialize(DataOutput index) throws IOException {
        for (String name : originalNames.keySet()) {
            StringStorageUtils.writeString(index, name);
            index.writeInt(originalNames.get(name));
        }
    }
    
    public void deserializeFromStream(DataInput index, int originalNameItems) throws IOException {
        originalNames.clear();
        for (int i = 0; i < originalNameItems; ++i) {
            String name = StringStorageUtils.readString(index);
            int weight = index.readInt();
            originalNames.put(name, weight);
        }
    }
    
    public void clear() {
        originalNames.clear();
    }
    
    public int size() {
        return originalNames.size();
    }
    
    public boolean isEmpty() {
        return originalNames.isEmpty();
    }
    
    public Integer getWeight(String name) {
        return originalNames.get(name);
    }
    
    public void setWeight(String name, int weight) {
        originalNames.put(name, weight);
    }
    
    public void addWeight(String name, int weight) {
        Integer previous = originalNames.get(name);
        originalNames.put(name, previous == null ? weight : previous + weight);
    }
    
    public String getTopName() {
        String topName = null;
        int weight = Integer.MIN_VALUE;
        for (String name : originalNames.keySet()) {
            int nameWeight = originalNames.get(name);
            if (nameWeight > weight) {
                weight = nameWeight;
                topName = name;
            }
        }
        return topName;
    }
    
    @SuppressWarnings("unchecked")
    public List<OriginalName> getSortedOriginalNames() {
        List<OriginalName> list = new ArrayList<OriginalName>(originalNames.size());
        for (String name : originalNames.keySet()) {
            list.add(new OriginalName(name, originalNames.get(name)));
        }
        Collections.sort(list);
        return list;
    }
    
    public void shrinkOriginalNames(int maxSize) {
        List<OriginalName> list = getSortedOriginalNames();
        list = list.subList(0, Math.min(maxSize, list.size()));
        originalNames.clear();
        for (OriginalName item : list) {
            originalNames.put(item.getName(), item.getWeight());
        }
    }
}
