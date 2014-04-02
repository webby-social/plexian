package net.webby.plexian.store;


import java.io.ByteArrayInputStream;
import java.io.DataInputStream;

import net.webby.plexian.store.FreeSpaceDirectory;
import junit.framework.TestCase;

public class FreeSpaceDirectoryTest extends TestCase {

    public void testSerialization() throws Exception {
        FreeSpaceDirectory dir = new FreeSpaceDirectory(null, 0);
        dir.addFreeSpace(5678, 15);
        int size = dir.calculateSize();
        byte[] blob = dir.serialize(size);
        assertEquals(blob.length, size);
        ByteArrayInputStream bin = new ByteArrayInputStream(blob);
        DataInputStream dis = new DataInputStream(bin);
        assertEquals(dis.readInt(), size);
        dir.deserializeFromStream(dis);
        assertEquals(dir.getFreeSpace(15).intValue(), 5678);
    }
}
