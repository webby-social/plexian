package net.webby.plexian.indexator.jpa;


import java.util.Map;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import net.webby.plexian.indexator.AbstractIndexator;
import net.webby.plexian.utils.PlexianUtils;

public class JPAIndexator extends AbstractIndexator {

    public static final int BATCH_SIZE = 1000;
    
    private PlatformTransactionManager transactionManager;
    
    @PersistenceContext
    private EntityManager entityManager;
    
    private int batchSize = BATCH_SIZE;
    
    public void setFields(Map<String, String> paramFields) {
        for (String plexianField : paramFields.keySet()) {
            Map<String, Set<String>> entities = PlexianUtils.parseObjectsFieldsExpression(paramFields.get(plexianField));
            JPAFieldIndexator indexator = new JPAFieldIndexator(entities, new TransactionTemplate(transactionManager), entityManager, batchSize);
            indexator.setField(plexianField);
            indexator.setPlexian(plexian);
            indexators.add(indexator);
        }
    }

    public void setEntityManager(EntityManager entityManager) {
        this.entityManager = entityManager;
    }

    public void setTransactionManager(PlatformTransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }
}