package com.oq_io.cs_challenge.utils;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;

import com.oq_io.cs_challenge.dao.Event;

public class DbConnect {
	private Session session;
	private int batchSize;
	private List<Event> eventListHolder = new ArrayList<Event>();
	private static Logger logger = LogManager.getLogger(DbConnect.class);
	
	public DbConnect() {
		
	}
	
	public Session getSession() {
		if(this.session == null || !this.session.isConnected()) {
			SessionFactory sF = DbSession.getSessionFactory();
			this.batchSize = sF.getSessionFactoryOptions().getJdbcBatchSize();
			this.session = sF.openSession();
		}
		return session;
	}
	
	public Transaction getTransaction() {
		Transaction transaction = null;
        try {
            transaction = getSession().beginTransaction();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return transaction;
	}
	
		
	
	public void Insert(Event event, Boolean hasMore) {
		getSession();
		//Add objects to holder for batch insertion
		if(eventListHolder.size() < batchSize && hasMore) {
			eventListHolder.add(event);
		}else if(eventListHolder.size() == batchSize || !hasMore) {
			eventListHolder.add(event);
			Insert(eventListHolder);
			eventListHolder.clear();
		}
	}
	
	
	public void Insert(List<Event> eventList) {
		logger.info("CALLING");
		Transaction transaction = getTransaction();
        try {
        	for (int i=0; i<eventList.size(); i++ ) {
        		getSession().save(eventList.get(i));
        		logger.debug("Saving "+eventList.get(i).getId());
        		if(i % batchSize == 0 || i == eventList.size() - 1) {
        			getSession().flush();
                	getSession().clear();
        		}
        	}
            transaction.commit();
        } catch (Exception e) {
            if (transaction != null) {
                transaction.rollback();
            }
            e.printStackTrace();
        }
	}
	
	//Generic hibernate object select *
	public <T, X> List<T> SelectDataObj(Class<T> classObject) {
		CriteriaBuilder builder = session.getCriteriaBuilder();
	    CriteriaQuery<T> criteria = builder.createQuery(classObject);
		Root<T> root = criteria.from(classObject);
		criteria.select(root);
		return session.createQuery(criteria).getResultList();
	}
	
	public int getBatchSize() {
		return batchSize;
	}
	

	
}
