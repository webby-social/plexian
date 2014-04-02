package net.sf.plexian.store;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Container implements FreeSpaceDirectoryProvider {

    private final Log log = LogFactory.getLog(this.getClass());
    
    private ContainerType type;
    private File dir;
    private IndexFileProvider indexFile;
    private FileSpaceManager fileSpaceManager;
    private RootDirectory rootDirectory;
    private long lastAccessTime = 0;
    private int operations = 0;
    
    public FreeSpaceDirectory getFreeSpaceDirectory() throws IOException {
        if (rootDirectory != null) {
            return rootDirectory.getFreeSpaceDirectory();
        }
        return null;
    }
    
    public Container(String directory, ContainerType type) throws IOException {
        this.type = type;
        dir = new File(directory);
        if (dir.exists() && !dir.isDirectory()) {
            throw new IOException("not a directory " + directory);
        }
        if (!dir.exists() && !dir.mkdir()) {
            throw new IOException("can't create directory " + directory);
        }
        if (type == ContainerType.NEW) {
            for (File file : dir.listFiles()) {
                if (!file.delete()) {
                    throw new IOException("cannot delete " + file);
                }
            }
        }
        indexFile = new IndexFileProvider(dir.getAbsolutePath(), type == ContainerType.READONLY);
        fileSpaceManager = new FileSpaceManager(indexFile, this, 0);
    }
    
    protected RootDirectory openRoot() throws IOException {
        if (rootDirectory == null) {
            rootDirectory = new RootDirectory(fileSpaceManager);
        }
        lastAccessTime = System.currentTimeMillis();
        operations++;
        return rootDirectory;
    }
    
    public void flush() {
        operations = 0;
        if (indexFile.isOpen()) {
            try {
                if (type != ContainerType.READONLY && rootDirectory != null) {
                    rootDirectory.store();
                }
            }
            catch(IOException e) {
                log.error("flush fail", e);
            }
            if (rootDirectory != null) {
                rootDirectory.clear();
            }
            rootDirectory = null;
        }
    }
    
   
    public void remove(String name) throws IOException {
        if (name.length() > 0) {
            RootDirectory root = openRoot();
            root.getRootNode().remove(name.toLowerCase(), root);            
        }
    }
    
    public void addWeight(String name, int weight) throws IOException {
        if (name.length() > 0) {
            RootDirectory root = openRoot();
            root.getRootNode().addWeight(name.toLowerCase(), weight, name, root);
        }
    }
    
    public void setWeight(String name, int weight) throws IOException {
        if (name.length() > 0) {
            RootDirectory root = openRoot();
            root.getRootNode().setWeight(name.toLowerCase(), weight, name, root);
        }
    }    
    
    public Integer getWeight(String name) throws IOException {
        return openRoot().getRootNode().getWeight(name.toLowerCase(), 0);
    }
    
    public List<OriginalName> getOriginalNames(String name) throws IOException {
        return openRoot().getRootNode().getOriginalNames(name.toLowerCase(), 0);
    }
    
    public List<String> searchLowerCase(String startsWith) throws IOException {
        return openRoot().getRootNode().search(startsWith.toLowerCase(), 0);
    }  
    
    public List<String> search(String startsWith) throws IOException {
        Node root = openRoot().getRootNode();
        List<String> lowerCaseResult = root.search(startsWith.toLowerCase(), 0);
        List<String> result = new ArrayList<String>(lowerCaseResult.size());
        for (String lowerCaseName : lowerCaseResult) {
            result.add(root.getTopOriginalName(lowerCaseName, 0));
        }
        return result;
    }  

    public List<String> searchByKeywords(String str) throws IOException {
        Keywords keywords = new Keywords(str);

        Node root = openRoot().getRootNode();
        
        StringBuilder prefix = new StringBuilder();
        
        for (int i = 0; i < keywords.size();) {
            Integer num = null;
            String joinedTerms = null;
            String joinedTermsLowerCase = null;
            
            for (int numTerms = keywords.size() - i; numTerms > 0; --numTerms) {
                joinedTerms = keywords.join(i, numTerms);
                joinedTermsLowerCase = joinedTerms.toLowerCase();
                if (root.hasWeightInChilds(joinedTermsLowerCase, 0)) {
                    num = numTerms;
                    break;
                }
            }
            
            int matched = 0;
            
            if (num == null) {
                joinedTerms = keywords.join(i, 1);
                joinedTermsLowerCase = joinedTerms.toLowerCase();
                matched = 1;
            }
            else {
                matched = num;
            }

            if (i + matched >= keywords.size()) {
                int prefixLength = prefix.length();
                List<String> lowerCaseResult = root.search(joinedTermsLowerCase, 0);
                List<String> result = new ArrayList<String>(lowerCaseResult.size());
                for (String lowerCaseName : lowerCaseResult) {
                    if (prefixLength > 0) {
                        prefix.append(' ');
                    }
                    prefix.append(root.getTopOriginalName(lowerCaseName, 0));
                    result.add(prefix.toString());
                    prefix.delete(prefixLength, prefix.length());
                }
                return result;
            }
            else {
                if (prefix.length() > 0) {
                    prefix.append(' ');
                }            
                if (num != null) {
                    prefix.append(root.getTopOriginalName(joinedTermsLowerCase, 0));
                }
                else {
                    prefix.append(joinedTerms);
                }
            }
            i += matched;
        }

        return Collections.emptyList();
    }     
    
    public void close() {
        flush();
        if (indexFile.isOpen()) {
            indexFile.close();
        }
    }

    public int countLoadedNodes() throws IOException {
        return openRoot().getRootNode().countLoadedNodes();
    }
    
    public long getIndexedItems() throws IOException {
        return openRoot().getTotalItems();
    }
    
    public void setReadOnly() {
        type = ContainerType.READONLY;
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public int getOperations() {
        return operations;
    }

    public boolean isReadOnly() {
        return type == ContainerType.READONLY;
    }
}

    