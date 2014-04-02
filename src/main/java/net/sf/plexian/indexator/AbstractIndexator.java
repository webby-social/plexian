package net.sf.plexian.indexator;

import java.util.ArrayList;
import java.util.List;

import net.sf.plexian.Plexian;

public abstract class AbstractIndexator {

    protected Plexian plexian;
    protected List<AbstractFieldIndexator> indexators = new ArrayList<AbstractFieldIndexator>();

    public AbstractFieldIndexator getFieldIndexator(String field) {
        for (AbstractFieldIndexator indexator : indexators) {
            if (indexator.getField().equals(field)) {
                return indexator;
            }
        }
        return null;
    }
    
    public boolean isBadField(String field) {
       return plexian.isBad(field);
    }
    
    public List<String> getRunningFields() {
        List<String> result = new ArrayList<String>();
        for (AbstractFieldIndexator indexator : indexators) {
            if (indexator.isRunning()) {
                result.add(indexator.getField());
            }
        }
        return result;
    }
    
    public String[] getSupportedFields() {
        String[] result = new String[indexators.size()];
        int i = 0;
        for (AbstractFieldIndexator indexator : indexators) {
            result[i++] = indexator.getField();
        }
        return result;
    }

    public void setPlexian(Plexian plexian) {
        this.plexian = plexian;
    }
}
