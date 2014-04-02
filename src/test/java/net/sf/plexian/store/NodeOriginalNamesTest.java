package net.sf.plexian.store;


import java.util.List;

import net.sf.plexian.store.NodeOriginalNames;
import net.sf.plexian.store.OriginalName;


import junit.framework.TestCase;

public class NodeOriginalNamesTest extends TestCase {

    public void testSetWeight() throws Exception {
        NodeOriginalNames names = new NodeOriginalNames();
        names.setWeight("moscow", 200);
        names.setWeight("moscow", 201);
        assertEquals(names.getWeight("moscow").intValue(), 201);
    }
    
    public void testAddWeight() throws Exception {
        NodeOriginalNames names = new NodeOriginalNames();
        names.addWeight("moscow", 200);
        names.addWeight("moscow", 201);
        assertEquals(names.getWeight("moscow").intValue(), 401);
    }
    
    public void testSort() throws Exception {
        NodeOriginalNames names = new NodeOriginalNames();
        names.setWeight("moscow", 200);
        names.setWeight("Moscow", 1201);
        names.setWeight("MOscow", 10);
        names.setWeight("MoscoW", 2);
        List<OriginalName> list = names.getSortedOriginalNames();
        assertEquals(4, list.size());
        assertEquals(list.get(0).getName(), "Moscow");
        assertEquals(list.get(0).getWeight(), 1201);
        assertEquals(list.get(3).getName(), "MoscoW");
        assertEquals(list.get(3).getWeight(), 2);
    }
    
    public void testShrink() throws Exception {
        NodeOriginalNames names = new NodeOriginalNames();
        names.setWeight("moscow", 200);
        names.setWeight("Moscow", 1201);
        names.setWeight("MOscow", 10);
        names.setWeight("MoscoW", 2);
        names.shrinkOriginalNames(2);
        List<OriginalName> list = names.getSortedOriginalNames();
        assertEquals(2, list.size());
        assertEquals(list.get(0).getName(), "Moscow");
        assertEquals(list.get(0).getWeight(), 1201);
        assertEquals(list.get(1).getName(), "moscow");
        assertEquals(list.get(1).getWeight(), 200);
    }
    
    public void testTopName() throws Exception {
        NodeOriginalNames names = new NodeOriginalNames();
        names.setWeight("moscow", 200);
        names.setWeight("Moscow", 1201);
        names.setWeight("MOscow", 10);
        names.setWeight("MoscoW", 2);
        assertEquals("Moscow", names.getTopName());
    }
    
}
