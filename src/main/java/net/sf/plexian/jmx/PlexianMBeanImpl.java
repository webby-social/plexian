package net.sf.plexian.jmx;

import net.sf.plexian.springframework.PlexianBean;

public class PlexianMBeanImpl implements PlexianMBean {

    private PlexianBean plexianBean;
    
    public PlexianMBeanImpl(PlexianBean plexianBean) {
        this.plexianBean = plexianBean;
        plexianBean.setUseJmx(true);
    }
    
    public long getAvgSearchMillis() {
        if (plexianBean.getSearchOperations() != 0) {
            return plexianBean.getSumSearchMills() / plexianBean.getSearchOperations();
        }
        return -1;
    }

    public int getBatchSize() {
        return plexianBean.getBatchSize();
    }

    public long getMaxSearchMillis() {
        return plexianBean.getMaxSearchMills();
    }

    public int getNumBadFileds() {
        return plexianBean.getBadFields().size();
    }

    public int getNumCloses() {
        return plexianBean.getNumCloses();
    }

    public int getNumFileds() {
        return plexianBean.getAllFields().size();
    }

    public int getNumReadonlyFields() {
        int count = 0;
        for (String field : plexianBean.getAllFields()) {
            count += plexianBean.isReadOnly(field) ? 1 : 0;
        }
        return count;
    }
    
    public int getNumFlushes() {
        return plexianBean.getNumFlushes();
    }

    public int getNumSearches() {
        return plexianBean.getSearchOperations();
    }

    public long getUnusedMillis() {
        return plexianBean.getUnusedMillis();
    }

    public int getLoadedNodes() {
        int nodes = 0;
        for (String field : plexianBean.getAllFields()) {
            nodes += plexianBean.countLoadedNodes(field);
        }
        return nodes;
    }
    
    public void setBatchSize(int batchSize) {
        plexianBean.setBatchSize(batchSize);
    }

    public void setUnusedMillis(long millis) {
        plexianBean.setUnusedMillis(millis);
    }

}
