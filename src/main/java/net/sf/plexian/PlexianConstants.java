package net.sf.plexian;


public class PlexianConstants {

    public final static int DEFAULT_BATCH_SIZE = 10000;
    
    public final static long DEFAULT_UNUSED_MILLIS = 300000;
    
    public final static int MAX_QUERY_ENTRIES = 10;
    
    public final static String STORE_CHARSET = "UTF-8";
    
    public final static int STORE_BLOCK_SIZE = 32;
    
    public final static int STORE_BLOCK_SEARCH_ITEMS = 32;
    
    public final static int READ_BLOCK_SIZE = 512;
    
    public final static int POINTER_SIZE = Integer.SIZE / 8;
    
    public final static byte[] ZERO_BLOCK = new byte[STORE_BLOCK_SIZE];

    public final static String INDEX_LOCK_FILE = "index.lock";
    
    public final static String READONLY_LOCK_FILE = "readonly.lock";

    public final static String INDEX_FILE = "index";
    
}
