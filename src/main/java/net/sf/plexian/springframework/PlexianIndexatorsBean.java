package net.sf.plexian.springframework;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import net.sf.plexian.indexator.AbstractFieldIndexator;
import net.sf.plexian.indexator.AbstractIndexator;
import net.sf.plexian.indexator.PlexianIndexators;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.InitializingBean;

public class PlexianIndexatorsBean implements InitializingBean, PlexianIndexators {

    private final Log log = LogFactory.getLog(getClass());

    public List<AbstractIndexator> indexators = Collections.emptyList();
    
    public void afterPropertiesSet() throws Exception {
        for (AbstractIndexator indexator : indexators) {
            for (String field : indexator.getSupportedFields()) {
                if (indexator.isBadField(field)) {
                    reindex(indexator, field);
                }
            }
        }
    }
    
    public Set<String> getRunningFields() {
        Set<String> result = new HashSet<String>();
        for (AbstractIndexator indexator : indexators) {
            for (String field : indexator.getRunningFields()) {
                result.add(field);
            }
        }
        return result;
    }
    
    public List<String> getSupportedFields() {
        List<String> result = new ArrayList<String>();
        for (AbstractIndexator indexator : indexators) {
            for (String field : indexator.getSupportedFields()) {
                result.add(field);
            }
        }
        Collections.sort(result);
        return result;
    }
    
    public void reindex(String field) {
        for (AbstractIndexator indexator : indexators) {
            for (String indexatorField : indexator.getSupportedFields()) {
                if (indexatorField.equals(field)) {
                    reindex(indexator, field);
                    return;
                }
            }
        }
        log.error("reindex field '" + field + "' not found");
    }
    
    private void reindex(AbstractIndexator indexator, String field) {
        AbstractFieldIndexator fieldIndexator = indexator.getFieldIndexator(field);
        if (!fieldIndexator.isRunning()) {
            (new Thread(fieldIndexator)).start();
        }
    }

    public void setIndexators(List<AbstractIndexator> abstractIndexators) {
        this.indexators = abstractIndexators;
    }
    
}
