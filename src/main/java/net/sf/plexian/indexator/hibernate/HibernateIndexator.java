package net.sf.plexian.indexator.hibernate;

import java.util.Map;
import java.util.Set;

import org.springframework.orm.hibernate3.HibernateOperations;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import net.sf.plexian.indexator.AbstractIndexator;
import net.sf.plexian.utils.PlexianUtils;

public class HibernateIndexator extends AbstractIndexator {

    public static final int BATCH_SIZE = 1000;
    
    private PlatformTransactionManager transactionManager;
    private HibernateOperations hibernateOperations;
    private int batchSize = BATCH_SIZE;
    
	public void setFields(Map<String, String> paramFields) {
        for (String plexianField : paramFields.keySet()) {
            Map<String, Set<String>> entities = PlexianUtils.parseObjectsFieldsExpression(paramFields.get(plexianField));
            HibernateFieldIndexator indexator = new HibernateFieldIndexator(entities, new TransactionTemplate(transactionManager), hibernateOperations, batchSize);
			indexator.setField(plexianField);
			indexator.setPlexian(plexian);
			indexators.add(indexator);
		}
	}

    public void setHibernateOperations(HibernateOperations hibernateOperations) {
        this.hibernateOperations = hibernateOperations;
    }

    public void setTransactionManager(PlatformTransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }


	
}
