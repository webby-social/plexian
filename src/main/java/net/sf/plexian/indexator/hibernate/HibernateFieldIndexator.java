package net.sf.plexian.indexator.hibernate;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import net.sf.plexian.IndexType;
import net.sf.plexian.PlexianConstants;
import net.sf.plexian.indexator.AbstractFieldIndexator;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.orm.hibernate3.HibernateOperations;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

public class HibernateFieldIndexator extends AbstractFieldIndexator {

	private final Log log = LogFactory.getLog(getClass());
    
    private TransactionTemplate transactionTemplate;
    private HibernateOperations hibernateOperations;
    private Map<String, Set<String>> entities;
    private int batchSize;
	
    class TransactionCaller implements TransactionCallback {

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
        
        public Object doInTransaction(TransactionStatus status) {
            String where = "";
            if (lastId != null) {
                where = " where id > '" + StringEscapeUtils.escapeSql(lastId.toString()) + "'";
            }
            String query = "select id, " + dbField + " from " + dbEntity + where + " order by id";
            Iterator i = hibernateOperations.iterate(query);
            int num = 0;
            Object id = null;
            while(i.hasNext()) {
                Object[] objs = (Object[]) i.next();
                if (objs[1] != null) {
                    plexian.index(field, objs[1].toString(), IndexType.ADD_FREQ, 1);
                }
                num++;
                if (num == batchSize) {
                    id = objs[0];
                    break;
                }
            }
            hibernateOperations.closeIterator(i);
            return id;
        }
    
    }
    
	HibernateFieldIndexator(Map<String, Set<String>> entities, TransactionTemplate transactionTemplate, HibernateOperations hibernateOperations, int batchSize) {
		this.entities = entities;
        this.transactionTemplate = transactionTemplate;
        this.hibernateOperations = hibernateOperations;
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
