package com.oq_io.cs_challenge.utils;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hibernate.Transaction;
import org.hibernate.Session;
import com.oq_io.cs_challenge.dao.Event;

public class EventProcessor {
	private DbConnect dbConnect;
	private AtomicInteger statsProcRows = new AtomicInteger();
	private long statsProcTime;
	private static Logger logger = LogManager.getLogger(EventProcessor.class);
	
	
	/**
	   * This constructor is used to process rows in the Event table 
	   * Pairing START and FINISH events and calculating duration and thresholded alert for FINISH logs
	   * @param dbConnect created database pool
	   */
	
	public EventProcessor(DbConnect dbConnect){
		this.dbConnect = dbConnect;
		processFinishEvents();
	}
	
	private void processFinishEvents() {
		String selectDuration = "select eF.id_pk, eF.timestamp - eS.timestamp as duration "
		        + " from Event eF "
		        + " join Event eS ON eF.id = eS.id AND eF.state = 'FINISHED' AND eS.state = 'STARTED'";
		long start = System.currentTimeMillis();
		Session session = dbConnect.getSession();
		Transaction tx = dbConnect.getTransaction();
		try {			
			@SuppressWarnings("unchecked")
			Stream<Object[]> durations = session.createQuery(selectDuration).stream();
			durations.forEach(m -> {
			        Event event = session.load(Event.class, (Integer) m[0]);
			        event.setDuration((Long) m[1]);
			        if((Long) m[1] > 4) {
			        	event.setAlert(true);
			        }
			        session.update(event);
			        //streaming update batching 
			        if(statsProcRows.incrementAndGet() % dbConnect.getBatchSize() == 0) {
			        	session.flush();
			        	session.clear();
		    		}		     
			});			
			tx.commit();
			this.statsProcTime = System.currentTimeMillis() - start;
		} catch (Exception e) {
            if (tx != null) {
                tx.rollback();
            }
            logger.error(e.getMessage());
            e.printStackTrace();
        }
	}
	
	public int getStatsProcRows() {
		return statsProcRows.get();
	}

	public long getStatsProcTime() {
		return statsProcTime;
	}
	
}
