package net.webby.plexian.indexator.jpa;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import net.webby.plexian.IndexType;
import net.webby.plexian.PlexianConstants;
import net.webby.plexian.indexator.AbstractFieldIndexator;

public class JPAFieldIndexator extends AbstractFieldIndexator {

    private final Log log = LogFactory.getLog(getClass());
    
    private TransactionTemplate transactionTemplate;
    private EntityManager entityManager;
    private Map<String, Set<String>> entities;
    private int batchSize;
    
    class TransactionCaller implements TransactionCallback<Object> {

        private String dbEntity;
        private String dbField;
        private int batchSize;
        private Object lastId;
        
        TransactionCaller(String entity, String field, int batchSize, Object lastId) {
            this.dbEntity = entity;
            this.dbField = field;
            this.batchSize = batchSize;
            this.lastId = lastId;
        }
        
        @Override
        public Object doInTransaction(TransactionStatus status) {
            String where = "";
            if (lastId != null) {
                where = " WHERE e.id > '" + StringEscapeUtils.escapeSql(lastId.toString()) + "'";
            }
            String jpql = "SELECT e.id, e." + dbField + " FROM " + dbEntity + " e" + where + " ORDER BY e.id";
            
            Query query = entityManager.createQuery(jpql)
                                       .setMaxResults(batchSize);
            
            List<Object[]> results = query.getResultList();
            
            Object id = null;
            for (Object[] result : results) {
                if (result[1] != null) {
                    plexian.index(field, result[1].toString(), IndexType.ADD_FREQ, 1);
                }
                id = result[0]; // Last processed ID
            }
            
            // Return last ID if we reached batch size (meaning there might be more records)
            return results.size() == batchSize ? id : null;
        }
    }
    
    JPAFieldIndexator(Map<String, Set<String>> entities, TransactionTemplate transactionTemplate, EntityManager entityManager, int batchSize) {
        this.entities = entities;
        this.transactionTemplate = transactionTemplate;
        this.entityManager = entityManager;
        this.batchSize = batchSize;
    }
    
    @Override
    protected void runInternal() {
        plexian.remove(field);
        plexian.setBatchSize(0);
        
        for (String dbEntity : entities.keySet()) {
            for (String dbField : entities.get(dbEntity)) {
                try {
                    Object lastId = null;
                    do {
                        lastId = transactionTemplate.execute(new TransactionCaller(dbEntity, dbField, batchSize, lastId));
                        plexian.flush(field);
                    } while(lastId != null);
                }
                catch(Exception e) {
                    log.error("index fail", e);
                }
            }
        }
        
        plexian.setBatchSize(PlexianConstants.DEFAULT_BATCH_SIZE);
    }
}