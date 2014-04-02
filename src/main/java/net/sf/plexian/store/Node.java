package net.sf.plexian.store;


import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.plexian.PlexianConstants;



public class Node extends Block {

    private static final int MAGIC = 0x5555;
    private static final int NODE_SIZE = Short.SIZE / 8;
    private static final int MAGIC_SIZE = Short.SIZE / 8;
    private static final int NODE_WEIGHT_SIZE = Integer.SIZE / 8;
    private static final int SUBNODES_SIZE = Short.SIZE / 8;
    private static final int QUEUE_SIZE = Byte.SIZE / 8;
    private static final int ORNAMES_SIZE = Short.SIZE / 8;
    
    private Node parent;

    private int nodeWeight = Integer.MIN_VALUE;
    private Map<Character, Node> subNodes;

    private NodeQueue queue;
    
    private NodeOriginalNames originalNames;
    
    private boolean lazy;
    private boolean persists;
    
    public int countLoadedNodes() {
        int num = 1;
        if (subNodes != null) {
            for (Character ch : subNodes.keySet()) {
                num += subNodes.get(ch).countLoadedNodes();
            }
        }
        return num;
    }
    
    public Node(FileSpaceManager fileSpaceManager, Node parent, long pos) {
        super(fileSpaceManager, pos, BlockSizeOfType.SHORT);
        this.parent = parent;
        if (pos == 0) {
            this.lazy = false;
            this.persists = false;
        }
        else {
            this.lazy = true;
            this.persists = true;
        }
    }
    
    private void ensureQueueExists() {
        if (queue == null) {
            queue = new NodeQueue();
        }
    }
    
    private void ensureOriginalNamesExists() {
        if (originalNames == null) {
            originalNames = new NodeOriginalNames();
        }
    }
    
    private void ensureSubNodesExists() {
        if (subNodes == null) {
            subNodes = new HashMap<Character, Node>();
        }
    }

    private Node getChildNode(Character ch) throws IOException {
        ensureSubNodesExists();
        Node child = subNodes.get(ch);
        if (child == null) {
            child = new Node(getFileSpaceManager(), this, 0);
            subNodes.put(ch, child);
        }
        return child;
    }
    
    public int addWeight(String name, int weight, String originalName, RootDirectory root) throws IOException {
        ensureLoaded();
        if (name.length() == 0) {
            if (nodeWeight == Integer.MIN_VALUE) {
                root.addTotalItems(1);
            }
            if (parent != null) {
                ensureQueueExists();
                queue.update(this, name, nodeWeight);
            }
            ensureOriginalNamesExists();
            originalNames.addWeight(originalName, weight);
            if (nodeWeight == Integer.MIN_VALUE) {
                nodeWeight = weight;
            }
            else {
                nodeWeight += weight;
            }
            persists = false;
            return nodeWeight;
        }
        else {
            Node child = getChildNode(name.charAt(0));
            int newWeight = child.addWeight(name.substring(1), weight, originalName, root);
            if (parent != null) {
                ensureQueueExists();
                queue.update(this, name, newWeight);
            } 
            persists = false;
            return newWeight;
        }
    }
    
    public void setWeight(String name, int weight, String originalName, RootDirectory root) throws IOException {
        ensureLoaded();
        if (name.length() == 0) {
            if (nodeWeight == Integer.MIN_VALUE) {
                root.addTotalItems(1);
            }
            if (parent != null) {
                ensureQueueExists();
                queue.update(this, name, weight);
            }
            ensureOriginalNamesExists();
            Integer prevWeight = originalNames.getWeight(originalName);
            originalNames.setWeight(originalName, weight);    
            nodeWeight = prevWeight == null ? weight : weight - prevWeight;
        }
        else {
            Node child = getChildNode(name.charAt(0));
            child.setWeight(name.substring(1), weight, originalName, root);
            if (parent != null) {
                ensureQueueExists();
                queue.update(this, name, weight);
            }
        }  
        persists = false;
    }

    private boolean isEmpty() {
        return (queue == null || queue.isEmpty()) && (originalNames == null || originalNames.isEmpty()) && (subNodes == null || subNodes.isEmpty());
    }
    
    public void remove(String name, RootDirectory root) throws IOException {
        ensureLoaded();
        if (name.length() > 0) {
            Node child = getChildNode(name.charAt(0));
            child.remove(name.substring(1), root);
            if (child.isEmpty()) {
                if (child.getPos() != 0L) {
                    getFileSpaceManager().deleteAllocation(child.getPos(), child.getSize());
                }
                subNodes.remove(name.charAt(0));
                child.clear();
            }
        }
        if (queue != null) {
            queue.remove(name);
        }
        if (name.length() == 0) {
            if (nodeWeight != Integer.MIN_VALUE) {
                root.addTotalItems(-1);
            }
            nodeWeight = Integer.MIN_VALUE;
            if (originalNames != null) {
                originalNames.clear();
            }
        }
        persists = false;
    }
    
    private int calculateSubNodesSize() {
        if (subNodes != null) {
            return subNodes.size() * (Character.SIZE / 8 + PlexianConstants.POINTER_SIZE);
        }
        else {
            return 0;
        }
    }
 
    public int calculateSize() {
        int size = NODE_SIZE + MAGIC_SIZE + NODE_WEIGHT_SIZE;
        size += SUBNODES_SIZE + calculateSubNodesSize();
        size += QUEUE_SIZE;
        if (queue != null) {
            size += queue.calculateSize();
        }
        size += ORNAMES_SIZE;
        if (originalNames != null) {
            size += originalNames.calculateSize();
        }
        return size;
    }
    
    private void ensureLoaded() throws IOException {
        if (lazy) {
            load();
            persists = true;
            lazy = false;
        }
    }    

    public long persist() throws IOException {
        if (!lazy && !persists) {
            store();
            persists = true;
        }
        return getPos();
    }
    
    public void deserializeFromStream(DataInputStream index) throws IOException {
        int magic = index.readShort();
        if (magic != MAGIC) {
            throw new IOException("invalid magic at " + getPos());
        }
        nodeWeight = index.readInt();
        int nodes = index.readShort();
        if (nodes < 0) {
            throw new IOException("bad file nodes less then zero");
        }
        for (int i = 0; i < nodes; ++i) {
            char ch = index.readChar();
            long pointer = PointerUtils.readPointer(index);
            if (pointer == 0) {
                throw new IndexOutOfBoundsException("readed zero pointer");
            }
            ensureSubNodesExists();
            subNodes.put(ch, new Node(getFileSpaceManager(), this, pointer));
        }
        int queueItems = index.read();
        if (queueItems < 0) {
            throw new IOException("bad file queue size less then zero");
        }
        if (queueItems > 0) {
            ensureQueueExists();
            queue.deserializeFromStream(index, queueItems);
        }
        int originalNameItems = index.readShort();
        if (originalNameItems < 0) {
            throw new IOException("bad file originalNameItems less then zero");
        }
        if (originalNameItems > 0) {
            ensureOriginalNamesExists();
            originalNames.deserializeFromStream(index, originalNameItems);
        }
    }
    
    public byte[] serialize(int size) throws IOException {
        ByteArrayOutputStream ba = new ByteArrayOutputStream(size);
        DataOutputStream index = new DataOutputStream(ba);
        index.writeShort(size);
        index.writeShort(MAGIC);
        index.writeInt(nodeWeight);
        if (subNodes != null) {
            if (subNodes.size() > (int)Short.MAX_VALUE) {
                throw new IndexOutOfBoundsException();
            }
            index.writeShort(subNodes.size());
            Character[] array = new Character[subNodes.size()];
            subNodes.keySet().toArray(array);
            Arrays.sort(array);
            for (Character ch : array) {
                index.writeChar(ch.charValue());
                long childPos = subNodes.get(ch).persist();
                if (childPos <= 0) {
                    throw new IOException("try to write invalid zero position");
                }
                PointerUtils.writePointer(index, childPos);
            }
        }
        else {
            index.writeShort(0);
        }
        if (queue != null) {
            if (queue.size() > (int)Byte.MAX_VALUE) {
                throw new IndexOutOfBoundsException();
            }        
            index.write(queue.size());
            queue.serialize(index);
        }
        else {
            index.write(0);
        }
        if (originalNames != null) {
            if (originalNames.size() > (int)Short.MAX_VALUE) {
                originalNames.shrinkOriginalNames((int)Short.MAX_VALUE);
            } 
            index.writeShort(originalNames.size());
            originalNames.serialize(index);
        }
        else {
            index.writeShort(0);
        }
        ba.close();
        return ba.toByteArray();
    }
    
    public List<String> search(String startsWith, int offset) throws IOException {
        ensureLoaded();
        int len = startsWith.length() - offset;
        if (len == 0) {
            return getEntries(startsWith, offset);
        }
        if (len >= 1 && subNodes != null) {
            Character ch = startsWith.charAt(offset);
            Node subNode = subNodes.get(ch);
            if (subNode != null) {
                List<String> result = subNode.search(startsWith, offset + 1);
                if (result.size() < PlexianConstants.MAX_QUERY_ENTRIES) {
                    List<String> addon = getEntries(startsWith, offset);
                    addon.removeAll(result);
                    int needToAddEntries = Math.min(addon.size(), PlexianConstants.MAX_QUERY_ENTRIES - result.size());
                    result.addAll(addon.subList(0, needToAddEntries));
                }
                return result;
            }
        }
        return new ArrayList<String>(PlexianConstants.MAX_QUERY_ENTRIES);
    }
    
    public boolean hasWeightInChilds(String startsWith, int offset) throws IOException {
        ensureLoaded();
        int len = startsWith.length() - offset;
        if (len == 0) {
            return (nodeWeight != Integer.MIN_VALUE) || subNodes.size() > 0;
        }
        if (len >= 1 && subNodes != null) {
            Character ch = startsWith.charAt(offset);
            Node subNode = subNodes.get(ch);
            if (subNode != null) {
                return subNode.hasWeightInChilds(startsWith, offset + 1);
            }
        }
        return false;
    }
    
    
    public Integer getWeight(String startsWith, int offset) throws IOException {
        ensureLoaded();
        int len = startsWith.length() - offset;
        if (len == 0) {
            return (nodeWeight == Integer.MIN_VALUE) ? null : nodeWeight;
        }
        if (len >= 1 && subNodes != null) {
            Character ch = startsWith.charAt(offset);
            Node subNode = subNodes.get(ch);
            if (subNode != null) {
                return subNode.getWeight(startsWith, offset + 1);
            }
        }
        return null;
    }
    
    public List<OriginalName> getOriginalNames(String startsWith, int offset) throws IOException {
        ensureLoaded();
        int len = startsWith.length() - offset;
        if (len == 0) {
            if (originalNames != null) {
                return originalNames.getSortedOriginalNames();
            }
            return Collections.emptyList();
        }
        if (len >= 1 && subNodes != null) {
            Character ch = startsWith.charAt(offset);
            Node subNode = subNodes.get(ch);
            if (subNode != null) {
                return subNode.getOriginalNames(startsWith, offset + 1);
            }
        }
        return Collections.emptyList();
    }
    
    public String getTopOriginalName(String startsWith, int offset) throws IOException {
        ensureLoaded();
        int len = startsWith.length() - offset;
        if (len == 0) {
            if (originalNames != null) {
                return originalNames.getTopName();
            }
            return null;
        }
        if (len >= 1 && subNodes != null) {
            Character ch = startsWith.charAt(offset);
            Node subNode = subNodes.get(ch);
            if (subNode != null) {
                return subNode.getTopOriginalName(startsWith, offset + 1);
            }
        }
        return null;
    }
    
    private List<String> getEntries(String startsWith, int offset) {
        if (queue != null) {
            return queue.getEntries(startsWith.substring(0, offset));
        }
        return new ArrayList<String>(PlexianConstants.MAX_QUERY_ENTRIES);
    }   
    
    protected void clear() {
        super.clear();
        parent = null;
        if (queue != null) {
            queue.clear();
            queue = null;
        }
        if (originalNames != null) {
            originalNames.clear();
            originalNames = null;
        }
        if (subNodes != null) {
            for (Character ch : subNodes.keySet()) {
                subNodes.get(ch).clear();
            }
            subNodes.clear();
            subNodes = null;
        }
    }
        
}
